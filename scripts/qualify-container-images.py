#!/usr/bin/env python3
"""Build and probe the eight pinned production images in an explicitly owned Colima VM.

No registry push, cluster mutation, or cloud operation. The check phase needs the
private owned TLS PostgreSQL fixture used by qualify-control-plane.py, already
migrated and granted. Empty controller bindings deliberately exercise startup,
idle reconciliation and shutdown, not backend jobs. API startup is explicitly
loopback development mode. Spark discovery is a local TLS protocol fixture.
"""
import argparse
import fnmatch
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tarfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

IMAGES = {
    "api": ("api", "Dockerfile", 58080),
    "execution": ("api", "ControllerDockerfile", 58081),
    "sql": ("api", "SQLControllerDockerfile", 58082),
    "search": ("api", "SearchControllerDockerfile", 58083),
    "sylon": ("api", "SylonControllerDockerfile", 58084),
    "publication": ("api", "PublicationControllerDockerfile", 58085),
    "workflow": ("api", "WorkflowControllerDockerfile", 58086),
    "web": ("web", "Dockerfile", 8080),
}
EXCLUDE = {".git", ".qa", ".tools", "node_modules", "dist", "bin", "coverage", "test-results", "playwright-report"}


def run(command, *, failure=False, **kwargs):
    result = subprocess.run(command, capture_output=True, **kwargs)
    if not failure and result.returncode:
        # Never print command argv or captured stderr: fixture commands can carry
        # private configuration even though normal qualification logs do not.
        raise RuntimeError("Qualification command failed; inspect the private log directory")
    return result


def package(directory):
    output = io.BytesIO()
    digest = hashlib.sha256()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for path in sorted(directory.rglob("*")):
            relative = path.relative_to(directory)
            if any(part in EXCLUDE or part == ".env" or part.startswith(".env.") for part in relative.parts):
                continue
            if path.is_symlink():
                raise ValueError("Build contexts must not include symbolic links")
            if not path.is_file() or fnmatch.fnmatch(path.name, "*.log") or fnmatch.fnmatch(path.name, "*.tsbuildinfo"):
                continue
            data = path.read_bytes()
            digest.update(str(relative).encode() + b"\0" + hashlib.sha256(data).digest())
            item = tarfile.TarInfo(str(relative))
            item.size, item.mode, item.mtime = len(data), 0o755 if os.access(path, os.X_OK) else 0o644, 0
            archive.addfile(item, io.BytesIO(data))
    return output.getvalue(), digest.hexdigest()


def remote_main(root):
    """Runs in the owned VM; stdout contains only a credential-free result."""
    config = json.loads((root / "private.json").read_text())
    active = []
    results = []
    discovery_requests = []
    token = (root / "discovery-token").read_text().strip()

    class Discovery(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/apis/spark.apache.org/v1" or self.headers.get("Authorization") != "Bearer " + token:
                self.send_response(403)
                self.end_headers()
                return
            discovery_requests.append(time.monotonic())
            data = json.dumps({"groupVersion": "spark.apache.org/v1", "resources": [{"name": "sparkapplications", "kind": "SparkApplication", "namespaced": True, "verbs": ["get", "create", "delete"]}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def log_message(self, *_):
            pass

    discovery = ThreadingHTTPServer(("127.0.0.1", 0), Discovery)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(root / "discovery.crt", root / "discovery.key")
    discovery.socket = context.wrap_socket(discovery.socket, server_side=True)
    thread = threading.Thread(target=discovery.serve_forever, daemon=True)
    thread.start()

    def nerd(*args, **kwargs):
        return run(["sudo", "nerdctl", *args], **kwargs)

    def image_name(kind):
        return config["images"][kind]["manifestDigest"]

    def launch(kind, env, *, suffix="", failure=False, expected_denial=None):
        name = "dataslate-oci-qualification-" + kind + suffix
        env_file = root / (name + ".env")
        env_file.write_text("".join(k + "=" + v + "\n" for k, v in env.items()))
        env_file.chmod(0o600)
        args = ["run", "--name", name, "--pull=never", "-d", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--memory=256m", "--cpus=1", "--pids-limit=128", "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=32m,mode=1777", "--mount", f"type=bind,src={root / 'runtime'},dst=/qualification,ro", "--env-file", str(env_file)]
        if kind == "api" and not failure:
            # Web shares this isolated network namespace. Only its HTTP port is
            # published, and only on VM loopback; the API itself stays loopback.
            args.extend(["--network=bridge", "--publish", "127.0.0.1:8080:8080"])
        elif kind == "web" and not failure:
            args.append("--network=container:dataslate-oci-qualification-api")
        elif kind in ("api", "web"):
            args.append("--network=none")
        else:
            args.append("--network=host")
        # Track before launch, so runtime errors/timeouts cannot leave an orphan.
        active.append(name)
        nerd(*args, image_name(kind), timeout=30)
        info = json.loads(nerd("inspect", name).stdout)[0]
        if failure:
            try:
                for _ in range(150):
                    info = json.loads(nerd("inspect", name).stdout)[0]
                    if not info["State"]["Running"]:
                        break
                    time.sleep(0.1)
                logs = nerd("logs", name)
                text = logs.stdout + logs.stderr
                (root / (name + ".log")).write_bytes(text)
                assert info["State"]["ExitCode"] == 1 and not info["State"]["Running"], kind + " did not reject invalid configuration"
                assert expected_denial and expected_denial.encode() in text, "Expected application rejection was not observed"
            finally:
                nerd("rm", "-f", name)
                active.remove(name)
            return None
        assert str(info["Config"]["User"]).split(":")[0] in ("101", "65532")
        assert info["HostConfig"]["ReadonlyRootfs"] is True
        status = dict(line.split(":", 1) for line in Path("/proc/" + str(info["State"]["Pid"]) + "/status").read_text().splitlines() if ":" in line)
        assert int(status["CapEff"].strip(), 16) == 0 and status["NoNewPrivs"].strip() == "1"
        assert all(uid in ("101", "65532") for uid in status["Uid"].split())
        return name

    def fetch(port, path):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{port}{path}", timeout=2) as response:
            return response.status, dict(response.headers), response.read(2 << 20)

    def wait_ready(kind):
        port = IMAGES[kind][2]
        for _ in range(100):
            try:
                if fetch(port, "/web-healthz" if kind == "web" else "/readyz")[0] == 200:
                    return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.1)
        raise RuntimeError(kind + " readiness failed")

    def stop(name):
        nerd("stop", "--time", "10", name, timeout=20)
        info = json.loads(nerd("inspect", name).stdout)[0]
        logs = nerd("logs", name)
        (root / (name + ".log")).write_bytes(logs.stdout + logs.stderr)
        assert info["State"]["ExitCode"] == 0, "Container did not stop gracefully"
        if "-api" not in name:
            assert b"ERROR" not in logs.stdout + logs.stderr and b"WARN" not in logs.stdout + logs.stderr
        nerd("rm", name)
        active.remove(name)

    try:
        for kind in IMAGES:
            image = json.loads(nerd("image", "inspect", image_name(kind)).stdout)[0]
            assert image["Id"] == config["images"][kind]["configDigest"], "Built image identity changed"
            results.append({"image": kind, "configDigest": image["Id"], "manifestDigest": image_name(kind), "repoDigests": image.get("RepoDigests", []), "user": image["Config"]["User"]})
        launch("api", {}, suffix="-denied", failure=True, expected_denial="OIDC mode requires")
        launch("api", {"DATASLATE_AUTH_MODE": "development", "DATASLATE_ADDR": "0.0.0.0:58080"}, suffix="-exposure-denied", failure=True, expected_denial="development mode requires a literal loopback listen address")
        api = launch("api", {"DATASLATE_AUTH_MODE": "development", "DATASLATE_ADDR": "127.0.0.1:58080"})
        web = launch("web", {"DATASLATE_API_UPSTREAM": "http://127.0.0.1:58080"})
        wait_ready("web")
        status, headers, html = fetch(8080, "/")
        assert status == 200 and b'<div id="root">' in html
        assert headers["X-Content-Type-Options"] == "nosniff" and headers["X-Frame-Options"] == "DENY"
        assert "script-src 'self'" in headers["Content-Security-Policy"]
        api_status = json.loads(fetch(8080, "/api/v1/status")[2])
        assert api_status["product"] == "DataSlate" and api_status["authMode"] == "development"
        api_info = json.loads(nerd("inspect", api).stdout)[0]
        assert api_info["HostConfig"]["PortBindings"]["8080/tcp"][0]["HostIp"] == "127.0.0.1"
        assert fetch(8080, "/readyz")[0] == 200
        assert fetch(8080, "/workspaces/qualification/notebooks/saved")[2] == html
        asset_paths = re.findall(rb'(?:src|href)="(/assets/[^"]+)"', html)
        assert asset_paths and all(fetch(8080, path.decode())[0] == 200 for path in asset_paths)
        fetch(8080, "/auth/callback?code=qualification-code-must-not-be-logged")
        web_logs = nerd("logs", web)
        assert b"qualification-code-must-not-be-logged" not in web_logs.stdout + web_logs.stderr
        stop(web)
        launch("web", {"DATASLATE_API_UPSTREAM": "http://user:password@localhost/inject"}, suffix="-denied", failure=True, expected_denial="DATASLATE_API_UPSTREAM must be an http(s) service origin")
        stop(api)
        for kind in ("execution", "sql", "search", "sylon", "publication", "workflow"):
            env = {"DATASLATE_DATABASE_URL": config["runtimeURL"]}
            if kind == "execution":
                env.update(DATASLATE_EXECUTION_BINDINGS_FILE="/qualification/empty-bindings.json", DATASLATE_CONTROLLER_ADDR="127.0.0.1:58081", DATASLATE_EXECUTION_NAMESPACES="dataslate-qualification", DATASLATE_KUBERNETES_API_URL=f"https://127.0.0.1:{discovery.server_port}", DATASLATE_KUBERNETES_CA_FILE="/qualification/discovery.crt", DATASLATE_KUBERNETES_TOKEN_FILE="/qualification/discovery-token")
            else:
                prefix = "DATASLATE_" + kind.upper()
                env.update({prefix + "_BINDINGS_FILE": "/qualification/empty-bindings.json", prefix + "_CONTROLLER_ADDR": "127.0.0.1:" + str(IMAGES[kind][2])})
                if kind == "publication":
                    env.update(DATASLATE_EXECUTION_BINDINGS_FILE="/qualification/empty-bindings.json", DATASLATE_SQL_BINDINGS_FILE="/qualification/empty-bindings.json")
            for attempt in range(2):
                name = launch(kind, env)
                wait_ready(kind)
                time.sleep(4.3)
                assert fetch(IMAGES[kind][2], "/healthz")[0] == 200
                assert fetch(IMAGES[kind][2], "/readyz")[0] == 200
                stop(name)
            insecure = urllib.parse.urlsplit(env["DATASLATE_DATABASE_URL"])
            env["DATASLATE_DATABASE_URL"] = urllib.parse.urlunsplit(insecure._replace(query="sslmode=disable"))
            launch(kind, env, suffix="-insecure-denied", failure=True, expected_denial="database connection failed")
        assert len(discovery_requests) >= 3
        result = {"images": results, "nonRootReadOnlyCapDropNoNewPrivileges": True, "apiDevelopmentLoopbackStartup": True, "apiProductionMisconfigurationRejected": True, "apiDevelopmentExposureRejected": True, "webAssetsSPAProxyHeadersAndCallbackLogRedaction": True, "webInvalidUpstreamRejected": True, "controllersVerifiedTLSIdleReconciliationRestartAndGracefulShutdown": ["execution", "sql", "search", "sylon", "publication", "workflow"], "controllersInsecureDatabaseRejected": True, "sparkDiscoveryProtocolFixture": True, "notTested": ["production OIDC login", "Spark operator execution", "live controller jobs", "cloud data services", "multi-node/HA", "amd64 images"]}
        (root / "startup-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
    finally:
        for name in active:
            logs = nerd("logs", name, failure=True)
            (root / (name + ".log")).write_bytes(logs.stdout + logs.stderr)
            nerd("rm", "-f", name, failure=True)
        discovery.shutdown()
        discovery.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("build", "check"), required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--component", choices=("all", "api", "web"), default="all")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--ssh-config", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"dataslate-[a-z0-9-]+qualification", args.profile):
        raise ValueError("An explicitly owned DataSlate qualification profile is required")
    repo = Path(__file__).resolve().parent.parent
    output = args.output_dir.resolve()
    remote_dir = "/tmp/" + output.name
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", output.name):
        raise ValueError("Output directory name must be a plain identifier")

    def ssh(*command, **kwargs):
        return run(["colima", "--profile", args.profile, "ssh", "--", *command], **kwargs)

    if args.mode == "build":
        components = ("api", "web") if args.component == "all" else (args.component,)
        if output.exists():
            evidence = json.loads((output / "build-summary.json").read_text())
            if evidence["profile"] != args.profile or evidence["remoteDirectory"] != remote_dir:
                raise ValueError("Existing build evidence does not match this VM/output directory")
        else:
            output.mkdir(parents=True, mode=0o700, exist_ok=False)
            ssh("mkdir", "-m", "700", remote_dir)
            evidence = {"profile": args.profile, "remoteDirectory": remote_dir, "platform": "linux/arm64", "sourceContextSHA256": {}, "builds": []}
        for component in components:
            if component in evidence["sourceContextSHA256"]:
                raise ValueError("Component already snapshotted; use a fresh output directory to rebuild")
            archive, digest = package(repo / component)
            evidence["sourceContextSHA256"][component] = digest
            ssh("mkdir", remote_dir + "/" + component)
            ssh("tar", "xf", "-", "-C", remote_dir + "/" + component, input=archive)
        for kind, (component, dockerfile, _) in IMAGES.items():
            if component not in components:
                continue
            log_path = output / (kind + "-build.log")
            print("Building " + kind, flush=True)
            with log_path.open("wb") as log:
                result = subprocess.run(["colima", "--profile", args.profile, "ssh", "--", "sudo", "nerdctl", "build", "--platform", "linux/arm64", "--progress", "plain", "-f", remote_dir + "/" + component + "/" + dockerfile, "-t", "dataslate-" + kind + ":container-qualification", remote_dir + "/" + component], stdout=log, stderr=log)
            if result.returncode:
                raise RuntimeError(kind + " image build failed; inspect private build log")
            log_text = log_path.read_text()
            manifest = re.findall(r"exporting manifest (sha256:[a-f0-9]{64})", log_text)[-1]
            config_digest = re.findall(r"exporting config (sha256:[a-f0-9]{64})", log_text)[-1]
            image = json.loads(ssh("sudo", "nerdctl", "image", "inspect", manifest).stdout)[0]
            assert image["Id"] == config_digest, "Build output and inspected image differ"
            evidence["builds"].append({"image": kind, "manifestDigest": manifest, "configDigest": config_digest, "dockerfile": component + "/" + dockerfile, "dockerfileSHA256": hashlib.sha256((repo / component / dockerfile).read_bytes()).hexdigest(), "passed": True})
            (output / "build-summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print("Selected image builds passed", flush=True)
        return

    evidence = json.loads((output / "build-summary.json").read_text())
    if evidence["profile"] != args.profile or evidence["remoteDirectory"] != remote_dir or len(evidence["builds"]) != len(IMAGES):
        raise ValueError("Build evidence must include all eight images for this VM/output directory")
    for component, expected in evidence["sourceContextSHA256"].items():
        if package(repo / component)[1] != expected:
            raise ValueError(component + " sources changed after image build; rebuild the final snapshot")
    if not args.fixture or not args.ssh_config:
        raise ValueError("check requires the private database fixture and VM SSH config")
    fixture = json.loads(args.fixture.read_text())
    url = urllib.parse.urlsplit(fixture["runtimeURL"])
    options = dict(urllib.parse.parse_qsl(url.query))
    if (fixture.get("owned") is not True or url.scheme not in ("postgres", "postgresql")
            or url.hostname != "127.0.0.1" or not url.port or options.get("sslmode") != "verify-full"
            or not re.fullmatch(r"dataslate_[a-z_]+qualification", fixture.get("database", ""))
            or not re.fullmatch(r"dataslate_[a-z_]+", fixture.get("role", ""))
            or url.path != "/" + fixture["database"] or url.username != fixture["role"]
            or not Path(options.get("sslrootcert", "")).is_file()
            or options["sslrootcert"] != fixture.get("caFile")):
        raise ValueError("An owned isolated loopback PostgreSQL fixture with verified TLS is required")
    # A reverse SSH tunnel preserves PostgreSQL's exact 127.0.0.1 TLS identity.
    remote_port = int(ssh("python3", "-c", "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()").stdout)
    authority = url.netloc.rsplit("@", 1)[0] + "@127.0.0.1:" + str(remote_port)
    options["sslrootcert"] = "/qualification/postgres.crt"
    runtime_url = urllib.parse.urlunsplit(url._replace(netloc=authority, query=urllib.parse.urlencode(options)))
    fixture_dir = output / "private"
    fixture_dir.mkdir(mode=0o700)
    (fixture_dir / "private.json").write_text(json.dumps({"runtimeURL": runtime_url, "images": {item["image"]: item for item in evidence["builds"]}}))
    (fixture_dir / "private.json").chmod(0o600)
    runtime_dir = fixture_dir / "runtime"
    runtime_dir.mkdir(mode=0o755)
    (runtime_dir / "postgres.crt").write_bytes(Path(fixture["caFile"]).read_bytes())
    (runtime_dir / "empty-bindings.json").write_text("[]\n")
    (fixture_dir / "discovery-token").write_text(os.urandom(32).hex())
    (runtime_dir / "discovery-token").write_bytes((fixture_dir / "discovery-token").read_bytes())
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(fixture_dir / "discovery.key"), "-out", str(fixture_dir / "discovery.crt"), "-days", "1", "-subj", "/CN=DataSlate disposable discovery fixture", "-addext", "subjectAltName=IP:127.0.0.1"], timeout=20)
    (runtime_dir / "discovery.crt").write_bytes((fixture_dir / "discovery.crt").read_bytes())
    for path in runtime_dir.iterdir():
        path.chmod(0o644)
    (fixture_dir / "qualify.py").write_bytes(Path(__file__).read_bytes())
    remote_fixture = remote_dir + "/fixture"
    ssh("mkdir", "-m", "700", remote_fixture)
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        for path in fixture_dir.iterdir():
            tar.add(path, arcname=path.name)
    ssh("tar", "xf", "-", "-C", remote_fixture, input=archive.getvalue())
    tunnel = subprocess.Popen(["ssh", "-F", str(args.ssh_config), "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "ExitOnForwardFailure=yes", "-N", "-R", f"127.0.0.1:{remote_port}:127.0.0.1:{url.port}", "lima-colima-" + args.profile], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(0.3)
        assert tunnel.poll() is None, "Database tunnel failed"
        result = ssh("python3", remote_fixture + "/qualify.py", "--remote", remote_fixture, failure=True, timeout=240)
        (output / "startup.log").write_bytes(result.stdout + result.stderr)
        # Preserve private diagnostic logs without emitting them.
        log_archive = "import pathlib,sys,tarfile; p=pathlib.Path(sys.argv[1]); t=tarfile.open(fileobj=sys.stdout.buffer,mode='w|'); [t.add(f,arcname=f.name) for f in sorted(p.glob('*.log')) if f.is_file()]; t.close()"
        logs = ssh("python3", "-c", log_archive, remote_fixture)
        (output / "diagnostics.tar").write_bytes(logs.stdout)
        if result.returncode:
            raise RuntimeError("OCI startup qualification failed; inspect the private diagnostic files")
        result = json.loads(result.stdout)
        (output / "startup-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        tunnel.terminate()
        try:
            tunnel.wait(timeout=5)
        except subprocess.TimeoutExpired:
            tunnel.kill()
        # Also cover a broken SSH session/runner timeout before its finally runs.
        names = ["dataslate-oci-qualification-" + kind for kind in IMAGES]
        names += ["dataslate-oci-qualification-api-denied", "dataslate-oci-qualification-api-exposure-denied", "dataslate-oci-qualification-web-denied"]
        names += ["dataslate-oci-qualification-" + kind + "-insecure-denied" for kind in ("execution", "sql", "search", "sylon", "publication", "workflow")]
        ssh("sudo", "nerdctl", "rm", "-f", *names, failure=True)
        # Generated private configs/keys are removed; diagnostic archives contain
        # only process logs, never environment files or the database fixture.
        ssh("rm", "-rf", remote_fixture, failure=True)
        shutil.rmtree(fixture_dir)


if __name__ == "__main__":
    if not __debug__:
        raise RuntimeError("Qualification requires Python assertions; optimization is forbidden")
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "--remote":
        remote_main(Path(sys.argv[2]))
    else:
        main()

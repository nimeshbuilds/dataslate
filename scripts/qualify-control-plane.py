#!/usr/bin/env python3
"""Qualify real controller binaries against an explicitly owned loopback TLS database.

The private fixture JSON contains owned=true, database, role, adminURL and runtimeURL.
It must identify a dedicated qualification database; this is not an install script.
Credentials are passed through process environments and never included in evidence.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def database_config(value):
    url = urllib.parse.urlsplit(value)
    options = dict(urllib.parse.parse_qsl(url.query))
    if (url.scheme not in ("postgres", "postgresql") or url.hostname != "127.0.0.1"
            or not url.port or options.get("sslmode") != "verify-full"
            or not Path(options.get("sslrootcert", "")).is_file()):
        raise ValueError("Only loopback PostgreSQL with an explicit verified CA is allowed")
    return url, options


def pg_environment(value):
    url, options = database_config(value)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env.update(PGHOST=url.hostname, PGPORT=str(url.port), PGDATABASE=url.path[1:],
               PGUSER=urllib.parse.unquote(url.username or ""),
               PGPASSWORD=urllib.parse.unquote(url.password or ""),
               PGSSLMODE="verify-full", PGSSLROOTCERT=options["sslrootcert"])
    return env


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    if not __debug__:
        raise RuntimeError("Qualification requires Python assertions; disable optimization")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--psql", required=True)
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text())
    admin, _ = database_config(fixture["adminURL"])
    runtime, _ = database_config(fixture["runtimeURL"])
    role, database = fixture["role"], fixture["database"]
    if (fixture.get("owned") is not True or not re.fullmatch(r"dataslate_[a-z_]+qualification", database)
            or not re.fullmatch(r"dataslate_[a-z_]+", role)
            or (admin.hostname, admin.port, admin.path) != (runtime.hostname, runtime.port, runtime.path)
            or runtime.path != "/" + database or runtime.username != role
            or admin.username == runtime.username):
        raise ValueError("An explicitly owned, isolated qualification database and distinct runtime role are required")
    args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    empty = args.output_dir / "empty-bindings.json"
    empty.write_text("[]\n")
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("DATASLATE_")}

    def sql(statement, *, runtime_role=False, failure=False):
        result = subprocess.run([args.psql, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", statement],
                                env=pg_environment(fixture["runtimeURL" if runtime_role else "adminURL"]),
                                capture_output=True, text=True, timeout=20)
        if failure:
            assert result.returncode != 0 and "permission denied" in result.stderr.lower(), "Expected a privilege denial"
        else:
            assert result.returncode == 0, "Database qualification statement failed"
        return result.stdout.strip()

    with (args.output_dir / "migration.log").open("w") as log:
        migration = subprocess.run([str(args.binaries / "dataslate-api"), "migrate"],
                                  env={**base_env, "DATASLATE_DATABASE_URL": fixture["adminURL"]},
                                  stdout=log, stderr=log, timeout=120)
    assert migration.returncode == 0, "Production migration binary failed"
    sql(f'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE ON SCHEMA public TO "{role}"; '
        f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO "{role}"; '
        f'GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"; '
        f'REVOKE INSERT,UPDATE,DELETE ON dataslate_schema_migrations FROM "{role}";')
    assert sql("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls "
               "FROM pg_roles WHERE rolname=current_user", runtime_role=True) == "f"
    assert sql("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", runtime_role=True) == "t"
    sql("CREATE TABLE dataslate_denied_ddl_probe(id integer)", runtime_role=True, failure=True)
    sql("DELETE FROM dataslate_schema_migrations WHERE false", runtime_role=True, failure=True)
    results = []
    for kind in ("sql", "search", "sylon", "publication"):
        binary = args.binaries / f"dataslate-{kind}-controller"
        selected_port = port()
        prefix = "DATASLATE_" + kind.upper()
        env = {**base_env, "DATASLATE_DATABASE_URL": fixture["runtimeURL"],
               prefix + "_BINDINGS_FILE": str(empty.resolve()),
               prefix + "_CONTROLLER_ADDR": f"127.0.0.1:{selected_port}"}
        if kind == "publication":
            env.update(DATASLATE_EXECUTION_BINDINGS_FILE=str(empty.resolve()), DATASLATE_SQL_BINDINGS_FILE=str(empty.resolve()))
        for attempt in range(2):
            log_path = args.output_dir / f"{kind}-{attempt}.log"
            with log_path.open("w") as log:
                process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
                try:
                    ready = False
                    for _ in range(80):
                        assert process.poll() is None, f"{kind} exited before readiness"
                        try:
                            with urllib.request.urlopen(f"http://127.0.0.1:{selected_port}/readyz", timeout=1) as response:
                                ready = response.status == 200
                        except (urllib.error.URLError, TimeoutError):
                            pass
                        if ready:
                            break
                        time.sleep(0.1)
                    assert ready, f"{kind} readiness timed out"
                    # Exercise more than one real database reconciliation tick.
                    time.sleep(4.3)
                    with urllib.request.urlopen(f"http://127.0.0.1:{selected_port}/readyz", timeout=2) as response:
                        assert response.status == 200
                finally:
                    stop(process)
            assert process.returncode == 0, f"{kind} did not shut down cleanly"
            log_text = log_path.read_text()
            assert "WARN" not in log_text and "ERROR" not in log_text, f"{kind} reconciliation reported an error"
        # Every production executable must reject an insecure database configuration.
        insecure = urllib.parse.urlsplit(fixture["runtimeURL"])
        insecure_dsn = urllib.parse.urlunsplit(insecure._replace(query="sslmode=disable"))
        with (args.output_dir / f"{kind}-insecure-denied.log").open("w") as log:
            denied = subprocess.run([str(binary)], env={**env, "DATASLATE_DATABASE_URL": insecure_dsn},
                                    stdout=log, stderr=log, timeout=15)
        denial_log = (args.output_dir / f"{kind}-insecure-denied.log").read_text().lower()
        assert denied.returncode == 1 and f"{kind} database connection failed" in denial_log, (
            f"{kind} did not report the expected application-level TLS rejection")
        results.append({"controller": kind, "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                        "verifiedTLS": True, "restrictedRole": True, "readinessAndIdleReconciliation": True,
                        "restart": True, "gracefulShutdown": True, "insecureDatabaseRejected": True})
    evidence = {"scope": "native production controller processes with owned loopback PostgreSQL",
                "controllers": results, "schemaVersion": int(sql("SELECT max(version) FROM dataslate_schema_migrations")),
                "runtimeDDLAndMigrationWritesDenied": True,
                "notTested": ["browser OIDC login", "cloud services", "complete Kubernetes install", "HA/restore", "live jobs"]}
    (args.output_dir / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence))


if __name__ == "__main__":
    main()

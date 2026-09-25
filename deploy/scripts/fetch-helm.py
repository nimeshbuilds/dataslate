#!/usr/bin/env python3
"""Fetch a pinned Helm into deploy/.cache only. Does not read Kubernetes credentials."""
import hashlib
import io
from pathlib import Path
import platform
import tarfile
import urllib.request

VERSION = "v4.3.0"
CHECKSUMS = {
    "darwin-arm64": "d3870437e1e95b67f8edbde964156c84a26503f560821d40c542441658934fba",
    "linux-amd64": "86584a54def73570558f66f5111cc53dfed56689637ae32c1201205d494f54fb",
}


def main():
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    target = platform.system().lower() + "-" + arch
    if target not in CHECKSUMS:
        raise SystemExit("No reviewed Helm checksum for " + target + "; install a verified Helm separately.")
    cache = Path(__file__).resolve().parents[1] / ".cache"
    cache.mkdir(exist_ok=True)
    url = f"https://get.helm.sh/helm-{VERSION}-{target}.tar.gz"
    with urllib.request.urlopen(url, timeout=60) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != CHECKSUMS[target]:
        raise SystemExit("Helm archive checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        # Read only the named executable, never extract archive paths.
        binary = files.extractfile(target + "/helm").read()
    output = cache / "helm"
    output.write_bytes(binary)
    output.chmod(0o755)
    print(output)


if __name__ == "__main__":
    main()

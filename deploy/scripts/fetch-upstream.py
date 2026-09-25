#!/usr/bin/env python3
"""Download checksum-locked optional upstream charts. Never installs anything."""
import hashlib
import json
from pathlib import Path
import urllib.request

root = Path(__file__).resolve().parents[1]
lock = json.loads((root / "bootstrap/upstream-lock.json").read_text())
cache = root / ".cache"
cache.mkdir(exist_ok=True)
for name, component in lock["components"].items():
    with urllib.request.urlopen(component["url"], timeout=60) as response:
        content = response.read()
    if hashlib.sha256(content).hexdigest() != component["sha256"]:
        raise SystemExit("Chart checksum mismatch: " + name)
    destination = cache / (name + ".tgz")
    destination.write_bytes(content)
    print(name + ": " + str(destination))

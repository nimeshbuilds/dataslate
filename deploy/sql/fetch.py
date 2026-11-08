#!/usr/bin/env python3
"""Fetch a checksum-locked Trino chart; never installs or contacts Kubernetes."""
import hashlib
import json
from pathlib import Path
import urllib.request
root=Path(__file__).resolve().parent
lock=json.loads((root/'upstream-lock.json').read_text())
destination=root.parent/'.cache/trino.tgz'
with urllib.request.urlopen(lock['url'],timeout=60) as response:
    data=response.read(8*1024*1024+1)
if len(data)>8*1024*1024 or hashlib.sha256(data).hexdigest()!=lock['sha256']:
    raise SystemExit('Trino chart size/checksum verification failed')
destination.parent.mkdir(parents=True,exist_ok=True)
destination.write_bytes(data)
print('Verified '+str(destination))

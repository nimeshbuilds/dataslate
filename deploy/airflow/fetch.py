#!/usr/bin/env python3
"""Download and checksum-verify the locked chart; never contact Kubernetes."""
import hashlib
import json
from pathlib import Path
import urllib.request

ROOT = Path(__file__).resolve().parent


def main():
    lock = json.loads((ROOT / 'upstream-lock.json').read_text())
    try:
        with urllib.request.urlopen(lock['url'], timeout=30) as response:
            data = response.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != lock['sha256']:
            raise ValueError()
    except Exception:
        raise SystemExit('Airflow chart download or checksum verification failed') from None
    destination = ROOT / '.cache' / 'airflow.tgz'
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    print('Verified Airflow chart: ' + str(destination))


if __name__ == '__main__':
    main()

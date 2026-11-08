#!/usr/bin/env python3
"""Prepare an allowlisted image context; never send the workspace or fixtures to a builder."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]


def prepare(destination):
    destination = Path(destination)
    if destination.exists():
        raise ValueError('Build context destination must not exist')
    lock = json.loads((ROOT / 'upstream-lock.json').read_text())
    constraints = ROOT / 'constraints-3.4.0-python3.12.txt'
    if hashlib.sha256(constraints.read_bytes()).hexdigest() != lock['constraintsSHA256']:
        raise ValueError('Constraints checksum mismatch')
    if 'FROM ' + lock['baseImage'] + '\n' not in (ROOT / 'Dockerfile').read_text():
        raise ValueError('Airflow base image does not match lock')
    files = [ROOT / p for p in ('Dockerfile', 'constraints-3.4.0-python3.12.txt', 'runtime_check.py', 'launch.py')]
    for package in ('sdk/python', 'integrations/airflow'):
        directory = REPO / package
        files.append(directory / 'pyproject.toml')
        files.extend(sorted((directory / 'src').glob('**/*.py')))
    if any(f.is_symlink() or not f.is_file() or f.stat().st_size > 512 * 1024 for f in files):
        raise ValueError('Unexpected source file in allowlisted build context')
    destination.mkdir(parents=True)
    for source in files:
        target = destination / source.relative_to(REPO)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return len(files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        count = prepare(args.output)
    except Exception:
        raise SystemExit('Airflow build context verification failed') from None
    print('Prepared allowlisted Airflow build context (' + str(count) + ' files)')

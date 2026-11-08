"""Build-time dependency fetch; runtime applications never access Maven."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tempfile
from urllib.request import urlopen


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def fetch(lock, directory, offline=False):
    entries = json.loads(lock.read_text())
    seen = set()
    directory.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        if set(entry) != {'name', 'url', 'sha256'} or not re.fullmatch(r'[A-Za-z0-9_.-]+\.jar', entry['name']) or entry['name'] in seen or not re.fullmatch(r'[a-f0-9]{64}', entry['sha256']) or not entry['url'].startswith('https://repo.maven.apache.org/maven2/') or entry['url'].rsplit('/', 1)[-1] != entry['name']:
            raise ValueError('invalid dependency lock')
        seen.add(entry['name'])
        target = directory/entry['name']
        if target.is_file() and digest(target) == entry['sha256']:
            continue
        if offline:
            raise ValueError('missing or changed locked jar: ' + entry['name'])
        with tempfile.NamedTemporaryFile(dir=directory, prefix='download-', delete=False) as tmp:
            pending = Path(tmp.name)
            try:
                with urlopen(entry['url'], timeout=60) as response:
                    for chunk in iter(lambda: response.read(1024 * 1024), b''):
                        tmp.write(chunk)
                tmp.close()
                if digest(pending) != entry['sha256']:
                    raise ValueError('checksum mismatch: ' + entry['name'])
                pending.replace(target)
            finally:
                pending.unlink(missing_ok=True)
    return len(entries)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lock', type=Path, default=Path(__file__).with_name('jars.lock.json'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    print('Verified', fetch(args.lock, args.directory, args.offline), 'locked jars')

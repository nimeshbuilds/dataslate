#!/usr/bin/env python3
"""Require each application Dockerfile base to match the reviewed digest lock."""
import json
from pathlib import Path
import re

root = Path(__file__).resolve().parents[2]
lock = json.loads((root/'deploy/base-images.lock.json').read_text())
allowed = {item['image']+'@'+item['digest'] for item in lock['images']}
if len(allowed)!=len(lock['images']) or any(not re.fullmatch(r'.+@sha256:[a-f0-9]{64}',image) for image in allowed):
    raise SystemExit('Invalid application base image lock')
paths = [*sorted((root/'api').glob('*Dockerfile')),root/'web/Dockerfile']
for path in paths:
    bases = re.findall(r'^FROM\s+(\S+)',path.read_text(),re.MULTILINE|re.IGNORECASE)
    if not bases or any(base not in allowed for base in bases):
        raise SystemExit('Unreviewed/missing application base image in '+str(path.relative_to(root)))
print('Application base-image lock verified for '+str(len(paths))+' Dockerfiles.')

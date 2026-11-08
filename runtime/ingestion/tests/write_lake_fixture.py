"""Create synthetic Delta fixtures for explicit local cross-engine qualification.

Run only in the qualified ingestion image with an empty disposable /lake volume.
This exercises local files, never production S3 credentials or the API's storage
allowlist. The timestamp fixture deliberately probes downstream precision loss.
"""
import json
from pathlib import Path
import sys

import dataslate_ingestion as runtime


def main():
    root = Path('/lake')
    if len(sys.argv) != 1 or not root.is_dir() or any(root.iterdir()):
        raise SystemExit('An empty disposable /lake mount is required')
    (root/'ownership.json').write_text(json.dumps({'profile': 'dataslate-lake-local-v1', 'synthetic': True}))
    receipts = {}
    for name, fields, rows in [
        ('orders', [('id', 'long'), ('region', 'string'), ('email', 'string'), ('amount', 'double'), ('active', 'boolean'), ('ordered_on', 'date')],
         [{'id': 1, 'region': 'US', 'email': 'one@example.invalid', 'amount': 10.5, 'active': True, 'ordered_on': '2026-11-07'},
          {'id': 2, 'region': 'EU', 'email': 'two@example.invalid', 'amount': 20.0, 'active': False, 'ordered_on': '2026-11-07'},
          {'id': 3, 'region': 'US', 'email': 'three@example.invalid', 'amount': 30.25, 'active': True, 'ordered_on': '2026-11-08'}]),
        ('timestamp_precision', [('id', 'long'), ('event_at', 'timestamp')],
         [{'id': 1, 'event_at': '2026-11-07T12:00:00.123456Z'}]),
    ]:
        source = root/(name+'-input'); source.mkdir()
        (source/'part1.json').write_text(''.join(json.dumps(row)+'\n' for row in rows))
        spec = {'version': 1, 'pipelineId': 'qualification-'+name, 'runId': 'qualification-'+name+'-run', 'pipelineRevision': 1,
                'receiptBindingSha256': 'b'*64, 'source': {'format': 'json', 'uri': source.as_uri()+'/',
                'schema': [{'name': field, 'type': kind, 'nullable': False} for field, kind in fields]},
                'destination': {'uri': (root/name).as_uri()+'/'}, 'checkpointUri': (root/(name+'-checkpoint')).as_uri()+'/',
                'maxFilesPerTrigger': 1, 'timeoutSeconds': 120}
        receipt = runtime.run(spec, allow_local_files=True, receipt_path=str(root/(name+'-receipt.json')))
        if receipt['state'] != 'succeeded' or not receipt['tableExists'] or receipt['table']['version'] != '0':
            raise RuntimeError('Synthetic table did not produce a verified first Delta snapshot')
        receipts[name] = receipt
    (root/'fixture-summary.json').write_text(json.dumps({'scope': 'synthetic local files only', 'receipts': receipts}, indent=2)+'\n')
    print('Created verified synthetic Delta tables; no external storage was contacted.')


if __name__ == '__main__':
    main()

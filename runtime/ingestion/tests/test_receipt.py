import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dataslate_ingestion as runtime
from test_contract import spec


class ReceiptTests(unittest.TestCase):
    def test_binding_is_required_strict_and_not_storage_contract(self):
        original = spec()
        for value in [None, '', 'a'*63, 'A'*64, 5, 'secret\n'+'a'*64]:
            changed = copy.deepcopy(original); changed['receiptBindingSha256'] = value
            with self.assertRaises(runtime.IngestionError):
                runtime.validate_spec(changed)
        changed = copy.deepcopy(original); del changed['receiptBindingSha256']
        with self.assertRaises(runtime.IngestionError):
            runtime.validate_spec(changed)
        changed = copy.deepcopy(original); changed['receiptBindingSha256'] = 'b'*64
        self.assertEqual(runtime.contract_for(original), runtime.contract_for(changed))

    def schema(self):
        return {'type': 'struct', 'fields': [dict(field, nullable=True, metadata={}) for field in spec()['source']['schema']]}

    def test_physical_schema_hash_uses_ordered_nullable_fields(self):
        schema = self.schema()
        expected = [{'name': 'id', 'nullable': True, 'type': 'long'}, {'name': 'name', 'nullable': True, 'type': 'string'}]
        digest = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(runtime._snapshot_schema_hash(json.dumps(schema), spec()['source']['schema']), digest)

    def test_schema_mismatch_and_hidden_metadata_fail_closed(self):
        for change in [lambda s: s['fields'].reverse(), lambda s: s['fields'][0].update(nullable=False),
                       lambda s: s['fields'][0].update(type='integer'), lambda s: s['fields'][0].update(metadata={'delta.columnMapping.id': 1}),
                       lambda s: s.update(type='array'), lambda s: s['fields'].append(s['fields'][0])]:
            schema = self.schema(); change(schema)
            with self.assertRaisesRegex(runtime.IngestionError, 'delta_schema_unverified'):
                runtime._snapshot_schema_hash(json.dumps(schema), spec()['source']['schema'])
        for malformed in ['null', '{}', '[]', '"customer-secret"', '{"type":"struct","type":"array","fields":[]}']:
            with self.assertRaises(runtime.IngestionError):
                runtime._snapshot_schema_hash(malformed, spec()['source']['schema'])

    def test_exact_size_write_and_oversize_is_never_truncated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'receipt'
            exact = {'text': 'x'*(runtime.RECEIPT_LIMIT-len('{"text":""}'))}
            runtime._write_receipt(path, exact)
            self.assertEqual(path.stat().st_size, runtime.RECEIPT_LIMIT)
            self.assertEqual(json.loads(path.read_bytes()), exact)
            before = path.read_bytes()
            with self.assertRaisesRegex(runtime.IngestionError, 'receipt_too_large'):
                runtime._write_receipt(path, {'text': exact['text']+'x'})
            self.assertEqual(path.read_bytes(), before)

    def test_write_failure_and_symlink_are_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'receipt'; link = Path(directory)/'link'
            path.write_text('preserve'); link.symlink_to(path)
            with self.assertRaisesRegex(runtime.IngestionError, 'receipt_write_failed'):
                runtime._write_receipt(link, {'state': 'succeeded'})
            self.assertEqual(path.read_text(), 'preserve')
            with patch.object(os, 'fsync', side_effect=OSError('private details')):
                with self.assertRaisesRegex(runtime.IngestionError, '^receipt_write_failed$'):
                    runtime._write_receipt(path, {'state': 'succeeded'})


if __name__ == '__main__':
    unittest.main()

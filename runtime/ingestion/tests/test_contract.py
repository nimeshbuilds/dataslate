import copy
import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dataslate_ingestion as runtime


def spec():
    return {'version': 1, 'pipelineId': 'pipe-123', 'pipelineRevision': 1, 'runId': 'run-123',
            'source': {'format': 'json', 'uri': 's3://example-bucket/raw/orders/', 'schema': [
                {'name': 'id', 'type': 'long', 'nullable': False}, {'name': 'name', 'type': 'string', 'nullable': True}]},
            'destination': {'uri': 's3://example-bucket/tables/orders/'},
            'checkpointUri': 's3://example-bucket/checkpoints/orders/', 'maxFilesPerTrigger': 10, 'timeoutSeconds': 60}


class MemoryStorage:
    def __init__(self):
        self.values = {}

    def exists(self, path):
        return path in self.values or any(k.startswith(path.rstrip('/') + '/') for k in self.values)

    def read_json(self, path):
        return copy.deepcopy(self.values[path])

    def write_json(self, path, value):
        if path in self.values:
            raise RuntimeError('refuse overwrite')
        self.values[path] = copy.deepcopy(value)

    def children(self, path):
        return [k[len(path):].split('/')[0] for k in self.values if k.startswith(path)]


class ContractTests(unittest.TestCase):
    def test_canonical_detached_contract(self):
        original = spec()
        normalized = runtime.validate_spec(original)
        self.assertEqual(normalized['source']['uri'], 's3a://example-bucket/raw/orders/')
        original['source']['schema'][0]['type'] = 'double'
        self.assertEqual(normalized['source']['schema'][0]['type'], 'long')

    def test_unknown_fields_and_bool_integers_rejected(self):
        for key, value in [('sql', 'select secret'), ('timeoutSeconds', True), ('pipelineRevision', False), ('maxFilesPerTrigger', 10001), ('version', True)]:
            with self.subTest(key=key):
                value_spec = spec(); value_spec[key] = value
                with self.assertRaises(runtime.IngestionError):
                    runtime.validate_spec(value_spec)

    def test_invalid_uri_classes(self):
        for uri in ['s3://user:secret@bucket/prefix/', 's3://bucket/a/?token=secret', 's3://bucket/', 's3://bucket/a/../b/', 's3://bucket/a//b/', 's3://bucket/a%2fb/', 's3://bucket/path', 's3://bucket/a*/', 'https://bucket/path/', 's3://bucket:443/path/', 'file:///tmp/input/']:
            with self.subTest(uri=uri), self.assertRaises(runtime.IngestionError):
                runtime.canonical_uri(uri)

    def test_local_requires_explicit_opt_in(self):
        self.assertEqual(runtime.canonical_uri('file:///tmp/input/', allow_local_files=True), 'file:///tmp/input/')

    def test_storage_prefix_overlap_after_alias_normalization(self):
        for target in ['s3a://example-bucket/raw/orders/', 's3://example-bucket/raw/', 's3://example-bucket/raw/orders/table/']:
            value = spec(); value['destination']['uri'] = target
            with self.subTest(target=target), self.assertRaisesRegex(runtime.IngestionError, 'overlapping_locations'):
                runtime.validate_spec(value)

    def test_schema_names_types_and_presence(self):
        for fields in [[], [{'name': 'id', 'type': 'integer', 'nullable': False}], [{'name': 'a b', 'type': 'long', 'nullable': False}], [{'name': 'ID', 'type': 'long', 'nullable': False}, {'name': 'id', 'type': 'long', 'nullable': True}], [{'name': 'id', 'type': 'long'}]]:
            value = spec(); value['source']['schema'] = fields
            with self.subTest(fields=fields), self.assertRaises(runtime.IngestionError):
                runtime.validate_spec(value)

    def test_restart_tuning_does_not_change_contract(self):
        a = runtime.validate_spec(spec()); b = copy.deepcopy(a)
        b.update(runId='another-run', pipelineRevision=2, timeoutSeconds=300, maxFilesPerTrigger=5)
        self.assertEqual(runtime.contract_for(a), runtime.contract_for(b))

    def test_json_exact_types_and_null(self):
        self.assertEqual(runtime.parse_json_record('{"id":42,"name":null}', spec()['source']['schema']), (42, None))

    def test_json_fail_closed(self):
        for line in ['{"id":1,"name":"a","secret":"x"}', '{"id":1}', '{"id":1,"id":2,"name":"a"}', '{"id":true,"name":"a"}', '{"id":"1","name":"a"}', '{"id":1.0,"name":"a"}', '{"id":null,"name":"a"}', '{"id":9223372036854775808,"name":"a"}', 'null', '[]', '{"id":NaN,"name":"a"}', '']:
            with self.subTest(line=line), self.assertRaises(runtime.IngestionError):
                runtime.parse_json_record(line, spec()['source']['schema'])

    def test_dates_timestamps_boolean_double(self):
        fields = [{'name': n, 'type': t, 'nullable': False} for n, t in [('d', 'date'), ('t', 'timestamp'), ('b', 'boolean'), ('v', 'double')]]
        result = runtime.parse_json_record('{"d":"2024-02-29","t":"2024-02-29T10:00:00+01:00","b":true,"v":4}', fields)
        self.assertEqual(result, (dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 9, tzinfo=dt.timezone.utc), True, 4.0))
        for key, value in [('d', '2023-02-29'), ('t', '2024-02-29T10:00:00'), ('b', 1), ('v', '4')]:
            import json
            record = {'d': '2024-02-29', 't': '2024-02-29T10:00:00Z', 'b': True, 'v': 4}; record[key] = value
            with self.subTest(key=key), self.assertRaises(runtime.IngestionError):
                runtime.parse_json_record(json.dumps(record), fields)

    def test_no_values_in_parse_error(self):
        with self.assertRaises(runtime.IngestionError) as error:
            runtime.parse_json_record('customer-secret:broken', spec()['source']['schema'])
        self.assertNotIn('customer-secret', str(error.exception))

    def test_contract_first_setup_and_restart(self):
        storage = MemoryStorage(); value = runtime.validate_spec(spec())
        digest = runtime._ensure_contract(storage, value)
        self.assertEqual(len(storage.values), 2)
        self.assertEqual(runtime._ensure_contract(storage, value), digest)

    def test_partial_setup_recoverable(self):
        storage = MemoryStorage(); value = runtime.validate_spec(spec())
        storage.values[value['destination']['uri'] + runtime.CONTRACT_FILE] = runtime.contract_for(value)
        runtime._ensure_contract(storage, value)
        self.assertEqual(len(storage.values), 2)

    def test_unowned_and_changed_contract_rejected(self):
        value = runtime.validate_spec(spec()); storage = MemoryStorage()
        storage.values[value['destination']['uri'] + 'existing.parquet'] = {}
        with self.assertRaisesRegex(runtime.IngestionError, 'unowned_location'):
            runtime._ensure_contract(storage, value)
        storage = MemoryStorage(); runtime._ensure_contract(storage, value)
        changed = copy.deepcopy(value); changed['source']['schema'][0]['type'] = 'double'
        with self.assertRaisesRegex(runtime.IngestionError, 'stored_contract_mismatch'):
            runtime._ensure_contract(storage, changed)

    def test_missing_checkpoint_after_commit_rejected(self):
        value = runtime.validate_spec(spec()); storage = MemoryStorage(); runtime._ensure_contract(storage, value)
        storage.values[value['destination']['uri'] + '_delta_log/000000.json'] = {}
        with self.assertRaisesRegex(runtime.IngestionError, 'checkpoint_missing'):
            runtime._ensure_contract(storage, value)

    def test_query_identity_bound_and_reset_refused(self):
        value = runtime.validate_spec(spec()); storage = MemoryStorage(); runtime._ensure_contract(storage, value)
        key = value['checkpointUri'] + 'metadata'; storage.values[key] = {'id': 'aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa'}
        runtime._ensure_query_identity(storage, value); runtime._ensure_query_identity(storage, value)
        storage.values[key] = {'id': 'bbbbbbbb-1111-2222-3333-bbbbbbbbbbbb'}
        with self.assertRaisesRegex(runtime.IngestionError, 'checkpoint_identity_changed'):
            runtime._ensure_query_identity(storage, value)

    def test_field_count_matches_api(self):
        value = spec(); value['source']['schema'] = [{'name': 'c'+str(i), 'type': 'long', 'nullable': False} for i in range(200)]
        runtime.validate_spec(value)
        value['source']['schema'].append({'name': 'extra', 'type': 'long', 'nullable': False})
        with self.assertRaisesRegex(runtime.IngestionError, 'invalid_schema'):
            runtime.validate_spec(value)

    def test_source_rejects_nested_delta_and_outside_prefix(self):
        storage = MemoryStorage()
        storage.values['s3a://bucket/source/nested/_delta_log/0.json'] = {}
        with self.assertRaisesRegex(runtime.IngestionError, 'delta_source_requires_table_reader'):
            runtime._plain_source_file(storage, 's3a://bucket/source/', 's3a://bucket/source/nested/part.parquet')
        with self.assertRaisesRegex(runtime.IngestionError, 'source_file_outside_prefix'):
            runtime._plain_source_file(storage, 's3a://bucket/source/', 's3a://bucket/source-other/part.parquet')

    def test_missing_identity_does_not_adopt_existing_table(self):
        value = runtime.validate_spec(spec()); storage = MemoryStorage(); runtime._ensure_contract(storage, value)
        storage.values[value['checkpointUri'] + 'metadata'] = {'id': 'aaaaaaaa-1111-2222-3333-aaaaaaaaaaaa'}
        storage.values[value['destination']['uri'] + '_delta_log/000000.json'] = {}
        with self.assertRaisesRegex(runtime.IngestionError, 'checkpoint_identity_missing'):
            runtime._ensure_query_identity(storage, value)


if __name__ == '__main__':
    unittest.main()

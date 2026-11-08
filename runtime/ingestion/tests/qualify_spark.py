"""Real JVM integration qualification; explicit invocation, never a silent skip.

JAVA_HOME and PySpark4.1.3 must be installed; PYSPARK_SUBMIT_ARGS supplies the
SHA-locked Delta jars. Only disposable local directories are used.
"""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dataslate_ingestion as runtime
from pyspark.sql import SparkSession, DataFrameWriter


class SparkQualification(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='dataslate-ingestion-qualify-')
        self.root = Path(self.tmp.name)
        (self.root/'source').mkdir()
        self.spec = {'version': 1, 'pipelineId': 'pipeline-test', 'runId': 'run-test', 'pipelineRevision': 1, 'receiptBindingSha256': 'a'*64,
                     'source': {'format': 'json', 'uri': (self.root/'source').as_uri()+'/', 'schema': [
                         {'name': 'id', 'type': 'long', 'nullable': False}, {'name': 'name', 'type': 'string', 'nullable': True}]},
                     'destination': {'uri': (self.root/'table').as_uri()+'/'},
                     'checkpointUri': (self.root/'checkpoint').as_uri()+'/', 'maxFilesPerTrigger': 1, 'timeoutSeconds': 120}

    def tearDown(self):
        current = SparkSession.getActiveSession()
        if current:
            current.stop()
        self.tmp.cleanup()

    def json_file(self, name, rows):
        (self.root/'source'/name).write_text(''.join(json.dumps(r)+'\n' for r in rows))

    def execute(self, spec=None):
        output = io.StringIO()
        try:
            with contextlib.redirect_stdout(output):
                receipt = runtime.run(spec or self.spec, allow_local_files=True, receipt_path=str(self.root/'receipt.json'))
        finally:
            lines = [line for line in output.getvalue().splitlines() if line.startswith(runtime.RECEIPT_PREFIX)]
            self.assertEqual(len(lines), 1)
            self.last_receipt = json.loads(lines[0][len(runtime.RECEIPT_PREFIX):])
            self.assertLessEqual(len(lines[0].removeprefix(runtime.RECEIPT_PREFIX).encode()), runtime.RECEIPT_LIMIT)
            self.assertIsNone(SparkSession.getActiveSession())
            print(lines[0], flush=True)
        self.assertEqual(receipt, self.last_receipt)
        self.assertEqual(json.loads((self.root/'receipt.json').read_text()), receipt)
        return receipt

    @contextlib.contextmanager
    def spark(self):
        session = (SparkSession.builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                   .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
        session.sparkContext.setLogLevel('ERROR')
        try:
            yield session
        finally:
            session.stop()

    def table_rows(self):
        with self.spark() as spark:
            return [tuple(r) for r in spark.read.format('delta').load(self.spec['destination']['uri']).orderBy('id').collect()]

    def test_01_json_incremental_restart_noop_and_snappy(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}, {'id': 2, 'name': None}])
        receipt = self.execute(); self.assertEqual(receipt['processedRows'], '2')
        self.assertTrue(receipt['tableExists'])
        table = receipt['table']
        self.assertEqual(table['version'], '0')
        self.assertEqual(table['minReaderVersion'], 1)
        self.assertEqual(table['minWriterVersion'], 2)
        self.assertEqual(table['readerFeatures'], [])
        self.assertEqual(table['writerFeatures'], [])
        metadata = next(json.loads(line)['metaData'] for line in (self.root/'table'/'_delta_log'/'00000000000000000000.json').read_text().splitlines() if 'metaData' in json.loads(line))
        self.assertEqual(table['id'], metadata['id'])
        self.assertEqual(table['schemaSha256'], runtime._snapshot_schema_hash(metadata['schemaString'], self.spec['source']['schema']))
        noop = self.execute()
        self.assertEqual(noop['processedRows'], '0')
        self.assertEqual(noop['table'], table)
        self.json_file('part2.json', [{'id': 3, 'name': 'three'}])
        # Late discovery of an old immutable file is not dropped by the default
        # one-week Spark seen-file horizon.
        os.utime(self.root/'source'/'part2.json', (1500000000, 1500000000))
        self.assertEqual(self.execute()['processedRows'], '1')
        self.assertEqual(self.table_rows(), [(1, 'one'), (2, None), (3, 'three')])
        with self.spark() as spark:
            jvm = spark._jvm; conf = spark.sparkContext._jsc.hadoopConfiguration()
            for file in (self.root/'table').glob('*.parquet'):
                inp = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(jvm.org.apache.hadoop.fs.Path(file.as_uri()), conf)
                reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(inp)
                try:
                    for block in reader.getFooter().getBlocks():
                        for column in block.getColumns():
                            self.assertEqual(str(column.getCodec()), 'SNAPPY')
                finally:
                    reader.close()

    def test_02_commit_then_lost_ack_replay_is_not_duplicate(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}, {'id': 2, 'name': 'two'}])
        original = DataFrameWriter.save
        injected = []
        def commit_then_fail(writer, path=None, *args, **kwargs):
            result = original(writer, path, *args, **kwargs)
            if path == self.spec['destination']['uri'].replace('file:///', 'file:///'):
                injected.append(True)
                raise RuntimeError('qualification failure after committed Delta write before stream acknowledgement')
            return result
        with patch.object(DataFrameWriter, 'save', commit_then_fail), self.assertRaisesRegex(Exception, 'qualification failure after committed Delta write'):
            self.execute()
        self.assertTrue(injected)
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertFalse((self.root/'checkpoint'/'commits'/'0').exists())
        self.assertEqual(self.table_rows(), [(1, 'one'), (2, 'two')])
        self.json_file('part2.json', [{'id': 3, 'name': 'three'}])
        self.assertEqual(self.execute()['state'], 'succeeded')
        self.assertEqual(self.table_rows(), [(1, 'one'), (2, 'two'), (3, 'three')])
        transactions = [x['txn'] for f in sorted((self.root/'table'/'_delta_log').glob('*.json')) for line in f.read_text().splitlines() if 'txn' in (x := json.loads(line))]
        self.assertEqual([x['version'] for x in transactions], [0, 1])

    def test_03_bad_json_batch_fails_without_partial_commit(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}])
        self.execute()
        self.json_file('part2.json', [{'id': 2, 'name': 'two'}, {'id': 3, 'name': 'three', 'extra': 'never discard me'}])
        with self.assertRaisesRegex(Exception, 'json_schema_drift'):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertEqual(self.table_rows(), [(1, 'one')])

    def test_04_parquet_success_and_empty_file_drift(self):
        self.spec['source']['format'] = 'parquet'
        with self.spark() as spark:
            frame = spark.createDataFrame([(1, 'one'), (2, 'two')], runtime._schema(self.spec['source']['schema']))
            frame.coalesce(1).write.parquet(str(self.root/'source'/'one'))
        self.assertEqual(self.execute()['processedRows'], '2')
        self.assertEqual(self.table_rows(), [(1, 'one'), (2, 'two')])
        with self.spark() as spark:
            spark.createDataFrame([], 'id long, name string, extra string').coalesce(1).write.parquet(str(self.root/'source'/'two'))
        with self.assertRaisesRegex(Exception, 'parquet_schema_drift'):
            self.execute()
        self.assertEqual(self.table_rows(), [(1, 'one'), (2, 'two')])

    def test_05_checkpoint_reset_and_changed_schema_fail(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}]); self.execute()
        altered = copy.deepcopy(self.spec); altered['source']['schema'][0]['type'] = 'double'
        with self.assertRaisesRegex(runtime.IngestionError, 'stored_contract_mismatch'):
            self.execute(altered)
        # Resetting only Spark checkpoint internals must not silently skip input.
        for child in (self.root/'checkpoint').iterdir():
            if child.name == runtime.CONTRACT_FILE or child.name == '.' + runtime.CONTRACT_FILE + '.crc':
                continue
            shutil.rmtree(child) if child.is_dir() else child.unlink()
        with self.assertRaisesRegex(runtime.IngestionError, 'checkpoint_missing'):
            self.execute()
        self.assertEqual(self.table_rows(), [(1, 'one')])

    def test_06_finite_transaction_retention_rejected(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}]); self.execute()
        with self.spark() as spark:
            spark.sql("ALTER TABLE delta.`" + self.spec['destination']['uri'] + "` SET TBLPROPERTIES ('delta.setTransactionRetentionDuration'='interval 1 day')")
        with self.assertRaisesRegex(runtime.IngestionError, 'transaction_retention_not_supported'):
            self.execute()

    def test_07_timeout_never_emits_success(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}])
        self.spec['timeoutSeconds'] = 1
        original = runtime._json_frame
        def slow_frame(*args):
            time.sleep(2)
            return original(*args)
        with patch.object(runtime, '_json_frame', slow_frame), self.assertRaises(Exception):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertEqual(self.last_receipt['errorCode'], 'ingestion_timeout')

    def test_08_checkpoint_uuid_swap_is_rejected(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}]); self.execute()
        metadata = self.root/'checkpoint'/'metadata'
        metadata.write_text(json.dumps({'id': 'bbbbbbbb-1111-2222-3333-bbbbbbbbbbbb'}))
        # Hadoop local checksum sidecar must match our intentional corruption.
        (metadata.parent/'.metadata.crc').unlink(missing_ok=True)
        self.json_file('part2.json', [{'id': 2, 'name': 'two'}])
        with self.assertRaisesRegex(Exception, 'checkpoint_identity_changed'):
            self.execute()
        self.assertEqual(self.table_rows(), [(1, 'one')])

    def test_09_unexpected_file_is_not_silently_ignored(self):
        (self.root/'source'/'part1.csv').write_text('1,one\n')
        with self.assertRaisesRegex(Exception, 'unexpected_source_file'):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertFalse((self.root/'table'/'_delta_log').exists())

    def test_10_stop_error_never_emits_success(self):
        original = SparkSession.stop
        def stop_then_error(session):
            original(session)
            raise RuntimeError('qualification stop acknowledgement lost')
        with patch.object(SparkSession, 'stop', stop_then_error), self.assertRaises(Exception):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertEqual(self.last_receipt['errorCode'], 'runtime_stop_uncertain')

    def test_11_invalid_utf8_is_not_replaced_silently(self):
        (self.root/'source'/'part1.json').write_bytes(b'{"id":1,"name":"bad\xff"}\n')
        with self.assertRaisesRegex(Exception, 'invalid_utf8'):
            self.execute()
        self.assertFalse((self.root/'table'/'_delta_log').exists())

    def test_12_delta_directory_is_not_treated_as_raw_parquet(self):
        self.spec['source']['format'] = 'parquet'
        with self.spark() as spark:
            spark.createDataFrame([(1, 'one')], runtime._schema(self.spec['source']['schema'])).write.format('delta').save(str(self.root/'source'/'nested_table'))
        with self.assertRaisesRegex(Exception, 'delta_source_requires_table_reader'):
            self.execute()
        self.assertFalse((self.root/'table'/'_delta_log').exists())

    def test_13_s3a_and_identity_classes_resolve_offline(self):
        with self.spark() as spark:
            loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
            for classname in ('org.apache.hadoop.fs.s3a.S3AFileSystem', 'software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider'):
                cls = spark._jvm.java.lang.Class.forName(classname, True, loader)
                self.assertGreater(len(cls.getDeclaredMethods()), 0)

    def test_14_empty_source_success_does_not_claim_table(self):
        receipt = self.execute()
        self.assertFalse(receipt['tableExists'])
        self.assertEqual(receipt['processedRows'], '0')
        self.assertIsNone(receipt['table'])

    def test_15_receipt_write_failure_has_no_success_marker(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}])
        (self.root/'receipt.json').mkdir()
        with self.assertRaisesRegex(runtime.IngestionError, 'receipt_write_failed'):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertEqual(self.last_receipt['errorCode'], 'receipt_write_failed')
        # Receipt failure never claims the previously committed batch rolled back.
        self.assertEqual(self.table_rows(), [(1, 'one')])

    def test_16_receipt_limit_is_failure_not_truncated_success(self):
        with patch.object(runtime, 'RECEIPT_LIMIT', 16), self.assertRaisesRegex(runtime.IngestionError, 'receipt_too_large'):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                runtime.run(self.spec, allow_local_files=True, receipt_path=str(self.root/'receipt.json'))
        receipts = [json.loads(line[len(runtime.RECEIPT_PREFIX):]) for line in output.getvalue().splitlines() if line.startswith(runtime.RECEIPT_PREFIX)]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]['state'], 'failed')
        self.assertFalse((self.root/'receipt.json').exists())

    def test_17_unverified_snapshot_cannot_emit_success(self):
        self.json_file('part1.json', [{'id': 1, 'name': 'one'}])
        with patch.object(runtime, '_snapshot_schema_hash', side_effect=runtime.IngestionError('delta_schema_unverified')), self.assertRaisesRegex(runtime.IngestionError, 'delta_schema_unverified'):
            self.execute()
        self.assertEqual(self.last_receipt['state'], 'failed')
        self.assertEqual(self.last_receipt['errorCode'], 'delta_schema_unverified')
        self.assertEqual(self.table_rows(), [(1, 'one')])


if __name__ == '__main__':
    if len(sys.argv) == 1:
        # Production uses a fresh driver JVM per run. Isolate scenarios so a
        # crashed JVM cannot turn later negative tests into false positives,
        # and repeated Spark context classloaders cannot exhaust the test heap.
        names = unittest.defaultTestLoader.getTestCaseNames(SparkQualification)
        failures = []
        for name in names:
            result = subprocess.run([sys.executable, __file__, 'SparkQualification.' + name], check=False)
            if result.returncode:
                failures.append(name)
        print(json.dumps({'realSparkScenarios': len(names), 'failed': failures}), flush=True)
        raise SystemExit(bool(failures))
    unittest.main(verbosity=2)

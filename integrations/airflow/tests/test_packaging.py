"""Exercise installed Airflow's provider discovery and executable DAG schema."""
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PackagingTests(unittest.TestCase):
    def test_reviewed_dependency_profile(self):
        lock = json.loads((ROOT / 'constraints.lock.json').read_text())
        self.assertEqual(hashlib.sha256((ROOT / 'constraints-3.12.txt').read_bytes()).hexdigest(), lock['sha256'])
        self.assertEqual(importlib.metadata.version('apache-airflow'), '3.4.0')
        self.assertEqual(importlib.metadata.version('dataslate'), '0.1.0a1')
        self.assertEqual(importlib.metadata.version('dataslate-airflow-provider'), '0.1.0a1')

    def test_installed_provider_and_connection_hook_are_discovered(self):
        from airflow.providers_manager import ProvidersManager
        from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime
        self.assertIn('dataslate-airflow-provider', ProvidersManager().providers)
        self.assertEqual(ProvidersManagerTaskRuntime().hooks['dataslate'].hook_class_name,
                         'dataslate_airflow.hooks.DataSlateHook')

    def test_example_serializes_with_real_airflow_and_correct_dependencies(self):
        from airflow.serialization.serialized_objects import DagSerialization
        spec = importlib.util.spec_from_file_location('dataslate_orders_example', ROOT / 'examples/orders_daily.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        dag = module.dag
        serialized = DagSerialization.to_dict(dag)  # Includes the pinned upstream JSON schema validation.
        self.assertEqual(len(serialized['dag']['tasks']), 6)
        self.assertEqual(dag.get_task('publish').upstream_task_ids, {'await_verified_ingestion'})
        self.assertEqual(dag.get_task('query').upstream_task_ids, {'await_publication'})
        self.assertEqual(dag.get_task('await_query').upstream_task_ids, {'query'})
        self.assertTrue(dag.get_task('await_verified_ingestion').multiple_outputs)
        self.assertIn('resourceId', str(dag.get_task('publish').request['run_id']))
        text = json.dumps(serialized)
        self.assertNotIn('access_token', text)
        self.assertNotIn('client_secret', text)


if __name__ == '__main__':
    unittest.main()

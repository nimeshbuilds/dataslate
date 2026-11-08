import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from build_context import prepare
from render import adapt, render, run_helm, settings, unique_fields
from runtime_check import TRUST_FILE, validate_database_url, validate_environment

ROOT = Path(__file__).resolve().parent


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'profile.example.json').read_text())

    def test_rejects_unknown_and_missing_fields(self):
        for variant in ({**self.config, 'executor': 'KubernetesExecutor'}, {k: v for k, v in self.config.items() if k != 'taskConnectionSecret'}):
            with self.assertRaises(ValueError):
                settings(variant)

    def test_rejects_invalid_identity_image_and_parallelism(self):
        for key, value in [('name', '../escape'), ('namespace', '{{ bad }}'), ('parallelism', True),
                           ('parallelism', 0), ('parallelism', 9), ('image', 'apache/airflow:latest'),
                           ('serviceAccount', 'default\n'), ('clientLabels', {})]:
            with self.subTest(key=key, value=value):
                config = {**self.config, key: value}
                with self.assertRaises(ValueError):
                    settings(config)

    def test_credential_secret_names_must_be_distinct(self):
        self.config['taskConnectionSecret'] = self.config['databaseSecret']
        with self.assertRaises(ValueError):
            settings(self.config)

    def test_git_requires_immutable_commit_and_no_embedded_credentials_or_template(self):
        for key, value in [('commit', 'main'), ('commit', 'f' * 39), ('subdir', '../dags'),
                           ('url', 'http://git.example.com/repo'), ('url', 'https://user:password@git.example.com/repo'),
                           ('url', 'https://git.example.com/{{lookup}}'), ('url', 'https://git.example.com/repo?token=secret')]:
            with self.subTest(key=key):
                config = copy.deepcopy(self.config); config['git'][key] = value
                with self.assertRaises(ValueError):
                    settings(config)

    def test_network_rejects_metadata_local_and_covering_supernets(self):
        for cidr in ['0.0.0.0/0', '128.0.0.0/1', '0.0.0.0/1', '127.0.0.0/8', '169.254.169.254/32',
                     '224.0.0.0/4', '::/0', '::1/128', 'fe80::/10', '::ffff:169.254.169.254/128']:
            with self.subTest(cidr=cidr):
                with self.assertRaises(ValueError):
                    settings({**self.config, 'egress': [{'cidr': cidr, 'port': 443}]})

    def test_no_kubernetes_operator_executor_or_default_accounts(self):
        values = settings(self.config)
        self.assertEqual('LocalExecutor', values['executor'])
        self.assertFalse(values['rbac']['create'])
        self.assertFalse(values['allowPodLaunching'])
        self.assertFalse(values['allowJobLaunching'])
        self.assertFalse(values['createUserJob']['enabled'])
        self.assertEqual(1, values['scheduler']['replicas'])
        self.assertEqual({'type': 'Recreate'}, values['scheduler']['strategy'])
        self.assertFalse(values['postgresql']['enabled'])
        self.assertFalse(values['redis']['enabled'])

    def test_checksum_mismatch_rejected_before_helm(self):
        with tempfile.NamedTemporaryFile() as chart:
            chart.write(b'changed chart'); chart.flush()
            with patch('render.run_helm') as helm, self.assertRaisesRegex(ValueError, 'checksum'):
                render(self.config, 'helm', chart.name)
            helm.assert_not_called()

    def test_renderer_never_echoes_tool_stderr(self):
        marker = 'SECRET_DIAGNOSTIC_MUST_NOT_ESCAPE'
        with self.assertRaises(ValueError) as error:
            run_helm([sys.executable, '-c', 'import sys;sys.stderr.write(' + repr(marker) + ');sys.exit(1)'])
        self.assertNotIn(marker, str(error.exception))

    def test_renderer_bounds_output(self):
        with self.assertRaisesRegex(ValueError, 'output limit'):
            run_helm([sys.executable, '-c', 'import sys;sys.stdout.write("x"*(9*1024*1024))'])

    def test_renderer_bounds_hung_child_process(self):
        with self.assertRaisesRegex(ValueError, 'time limit'):
            run_helm([sys.executable, '-c', 'import subprocess,sys;subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"])'], timeout=0.3)

    def test_duplicate_json_fields_are_not_accepted(self):
        with self.assertRaises(ValueError):
            json.loads('{"git":{"url":"first","url":"second"}}', object_pairs_hook=unique_fields)

    def test_lock_constraints_and_build_context_are_bounded_and_secret_free(self):
        lock = json.loads((ROOT / 'upstream-lock.json').read_text())
        self.assertEqual(lock['constraintsSHA256'], hashlib.sha256((ROOT / 'constraints-3.4.0-python3.12.txt').read_bytes()).hexdigest())
        self.assertIn('FROM ' + lock['baseImage'], (ROOT / 'Dockerfile').read_text())
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'context'
            count = prepare(destination)
            files = [p for p in destination.rglob('*') if p.is_file()]
            self.assertEqual(count, len(files))
            for path in files:
                self.assertNotIn('.qa', path.parts)
                self.assertNotIn('.env', path.name)
                self.assertTrue(path.suffix in ('.py', '.toml', '.txt') or path.name == 'Dockerfile')
            with self.assertRaises(ValueError):
                prepare(destination)


class RuntimeContractTests(unittest.TestCase):
    def setUp(self):
        self.uri = 'postgresql+psycopg://airflow:secret@db.example.com:5432/airflow?sslmode=verify-full&sslrootcert=' + TRUST_FILE
        self.env = {'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': self.uri,
                    'AIRFLOW__CORE__FERNET_KEY': base64.urlsafe_b64encode(b'f' * 32).decode(),
                    'AIRFLOW__API__SECRET_KEY': 's' * 64, 'AIRFLOW__API_AUTH__JWT_SECRET': 'j' * 64}

    def test_valid_psycopg_uri(self):
        validate_database_url(self.uri)

    def test_weak_tls_wrong_driver_and_conflicting_parameters_rejected_without_secrets(self):
        variants = [self.uri.replace('verify-full', 'require'), self.uri.replace('verify-full', 'disable'),
                    self.uri.replace('+psycopg', ''), self.uri + '&sslmode=disable', self.uri + '&host=other',
                    self.uri.replace(TRUST_FILE, '/tmp/untrusted.pem'), 'not-a-database-secret-value']
        for uri in variants:
            with self.subTest(uri=uri), self.assertRaises(ValueError) as error:
                validate_database_url(uri)
            self.assertNotIn('secret', str(error.exception))
            self.assertNotIn(uri, str(error.exception))

    def test_overrides_cannot_replace_checked_tls_uri(self):
        for base in ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC', 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS',
                     'AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS_ASYNC', 'AIRFLOW__DATABASE__SQL_ALCHEMY_ENGINE_ARGS']:
            for suffix in ['', '_CMD', '_SECRET']:
                with self.subTest(key=base + suffix), self.assertRaises(ValueError):
                    validate_environment({**self.env, base + suffix: 'SECRET_OVERRIDE'})

    def test_missing_or_short_signing_keys_rejected(self):
        for key in ('AIRFLOW__API__SECRET_KEY', 'AIRFLOW__API_AUTH__JWT_SECRET', 'AIRFLOW__CORE__FERNET_KEY'):
            for env in ({k: v for k, v in self.env.items() if k != key}, {**self.env, key: 'short'}):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    validate_environment(env, require_jwt=True)

    def test_trust_bundle_is_loaded_and_missing_or_bad_trust_fails(self):
        with patch('runtime_check.ssl.create_default_context') as create:
            validate_environment(self.env, require_jwt=True)
            create.assert_called_once_with(cafile=TRUST_FILE)
        with patch('runtime_check.ssl.create_default_context', side_effect=OSError), self.assertRaises(OSError):
            validate_environment(self.env)


class RealChartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = json.loads((ROOT / 'profile.example.json').read_text())
        helm = os.environ.get('HELM_BIN')
        chart = Path(os.environ.get('AIRFLOW_CHART', ROOT / '.cache/airflow.tgz'))
        if not helm or not chart.is_file():
            raise unittest.SkipTest('Set HELM_BIN and fetch the pinned Airflow chart for real chart tests')
        cls.docs = render(cls.config, helm, chart)

    def test_actual_chart_owns_only_expected_namespaced_resources(self):
        counts = {}
        for doc in self.docs:
            counts[doc['kind']] = counts.get(doc['kind'], 0) + 1
            self.assertEqual(self.config['namespace'], doc['metadata']['namespace'])
        self.assertEqual({'Deployment': 3, 'Job': 1, 'ConfigMap': 2, 'Service': 1, 'NetworkPolicy': 2}, counts)

    def test_actual_pod_security_volumes_and_credential_boundaries(self):
        for doc in self.docs:
            if doc['kind'] not in ('Deployment', 'Job'):
                continue
            pod = doc['spec']['template']['spec']
            component = doc['spec']['template']['metadata']['labels']['component']
            self.assertFalse(pod['automountServiceAccountToken'])
            self.assertEqual(self.config['serviceAccount'], pod['serviceAccountName'])
            for container in pod.get('initContainers', []) + pod['containers']:
                self.assertEqual(self.config['image'], container['image'])
                security = container['securityContext']
                self.assertTrue(security['readOnlyRootFilesystem'])
                self.assertFalse(security['allowPrivilegeEscalation'])
                self.assertEqual(['ALL'], security['capabilities']['drop'])
                env = {e['name']: e for e in container.get('env', [])}
                if 'AIRFLOW_CONN_DATASLATE_DEFAULT' in env:
                    self.assertEqual(('scheduler', 'scheduler'), (component, container['name']))
                if 'AIRFLOW_CONN_DATASLATE_GIT' in env:
                    self.assertIn(container['name'], ('scheduler', 'dag-processor'))
                paths = [m['mountPath'] for m in container.get('volumeMounts', [])]
                self.assertEqual(len(paths), len(set(paths)))
                if '/etc/airflow-server-tls' in paths:
                    self.assertEqual('api-server', container['name'])

    def test_actual_api_is_https_and_uses_pinned_git_and_fab(self):
        api = next(d for d in self.docs if d['kind'] == 'Deployment' and d['metadata']['name'].endswith('-api-server'))
        container = api['spec']['template']['spec']['containers'][0]
        self.assertIn('--ssl-key', container['args'])
        self.assertEqual('1', container['args'][container['args'].index('--workers') + 1])
        for key in ('livenessProbe', 'readinessProbe', 'startupProbe'):
            self.assertEqual('HTTPS', container[key]['httpGet']['scheme'])
        config = next(d for d in self.docs if 'airflow.cfg' in d.get('data', {}))['data']['airflow.cfg']
        self.assertIn('auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager', config)
        self.assertIn('client_use_public_certs = False', config)
        self.assertIn('https://dataslate-airflow-api-server.dataslate-orchestration.svc:8080/execution/', config)
        self.assertIn(self.config['git']['commit'], config)
        self.assertNotIn('LocalDagBundle', config)

    def test_migration_is_explicit_bounded_and_has_no_helm_hooks(self):
        job = next(d for d in self.docs if d['kind'] == 'Job')
        self.assertEqual(0, job['spec']['backoffLimit'])
        self.assertEqual(600, job['spec']['activeDeadlineSeconds'])
        self.assertEqual('Never', job['spec']['template']['spec']['restartPolicy'])
        self.assertNotIn('helm.sh/hook', job['metadata'].get('annotations', {}))

    def test_unexpected_chart_security_changes_fail_closed(self):
        unexpected = copy.deepcopy(self.docs)
        unexpected = [d for d in unexpected if d['kind'] != 'NetworkPolicy']
        unexpected.append({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'unexpected'}})
        with self.assertRaises(ValueError):
            adapt(unexpected, self.config)
        changed = [copy.deepcopy(d) for d in self.docs if d['kind'] != 'NetworkPolicy']
        next(d for d in changed if d['kind'] == 'Deployment')['spec']['template']['spec']['containers'][0]['image'] = 'bad:latest'
        with self.assertRaises(ValueError):
            adapt(changed, self.config)

    def test_ingress_is_limited_to_https_api_and_no_task_log_port(self):
        policies = [d for d in self.docs if d['kind'] == 'NetworkPolicy']
        for policy in policies:
            for rule in policy['spec'].get('ingress', []):
                self.assertEqual([{'protocol': 'TCP', 'port': 8080}], rule['ports'])
                for source in rule['from']:
                    self.assertIn('namespaceSelector', source)
                    self.assertIn('podSelector', source)


if __name__ == '__main__':
    unittest.main()

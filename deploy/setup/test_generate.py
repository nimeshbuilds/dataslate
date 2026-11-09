import copy
import importlib.util
import json
import os
import shlex
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('setup_generate', HERE / 'generate.py')
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
EXAMPLE = json.loads((HERE / 'profile.example.json').read_text())


class ProfileTests(unittest.TestCase):
    def profile(self):
        return copy.deepcopy(EXAMPLE)

    def test_valid_pins_separate_credentials_and_network(self):
        values = g.validate(self.profile())
        self.assertEqual(values['migrations']['databaseSecret']['name'], 'dataslate-migration-database')
        self.assertEqual(values['postgres']['external']['secretName'], 'dataslate-runtime-database')
        self.assertTrue(values['api']['image']['digest'].startswith('sha256:'))
        self.assertEqual(values['networkPolicy']['httpsEgressCidrs'], ['203.0.113.10/32'])

    def test_optional_oidc_ca_is_not_global(self):
        profile = self.profile()
        profile['oidc']['caSecret'] = {'name': 'identity-ca', 'key': 'root.pem'}
        values = g.validate(profile)
        self.assertEqual(values['oidc']['caSecret'], profile['oidc']['caSecret'])
        self.assertNotEqual(values['postgres']['external']['caSecretName'], 'identity-ca')

    def test_unknown_secret_or_arbitrary_fields_rejected(self):
        for path in [(), ('database',), ('oidc',), ('images',), ('network',), ('ingress',), ('database', 'runtimeSecret')]:
            with self.subTest(path=path):
                profile = self.profile()
                target = profile
                for key in path:
                    target = target[key]
                target['password'] = 'never-print-this-secret'
                with self.assertRaises(g.Invalid) as error:
                    g.validate(profile)
                self.assertNotIn('never-print', str(error.exception))

    def test_roles_and_secret_boundaries(self):
        for key, value in [('runtimeRole', 'dataslate_migrator'), ('runtimeRole', 'runtime;DROP ROLE owner'), ('name', 'db\nother')]:
            profile = self.profile()
            profile['database'][key] = value
            with self.assertRaises(g.Invalid):
                g.validate(profile)
        profile = self.profile()
        profile['database']['runtimeSecret']['name'] = profile['database']['migrationSecret']['name']
        with self.assertRaises(g.Invalid):
            g.validate(profile)

    def test_mutable_or_malformed_images_rejected(self):
        for image in ['registry.example.com/api:latest', 'repo@sha256:' + 'a'*64, 'registry/repo@sha256:wrong', 'registry/../repo@sha256:' + 'a'*64]:
            profile = self.profile()
            profile['images']['api'] = image
            with self.assertRaises(g.Invalid):
                g.validate(profile)

    def test_oidc_https_and_distinct_audience(self):
        for issuer in ['http://identity.example.com', 'https://user:password@identity.example.com', 'https://identity.example.com?token=secret', 'https://identity.example.com/#fragment', 'https://identity.example.com/$(echo hacked)']:
            profile = self.profile()
            profile['oidc']['issuer'] = issuer
            with self.assertRaises(g.Invalid):
                g.validate(profile)
        profile = self.profile()
        profile['oidc']['audience'] = profile['oidc']['clientId']
        with self.assertRaises(g.Invalid):
            g.validate(profile)

    def test_public_origin_only(self):
        for origin in ['https://example.com/path', 'https://example.com:8443', 'https://example.com/?token=secret']:
            profile = self.profile()
            profile['publicUrl'] = origin
            with self.assertRaises(g.Invalid):
                g.validate(profile)

    def test_oidc_scopes_and_port_must_be_explicit(self):
        for scopes in ['profile', 'openid openid', 'openid\nprofile']:
            profile = self.profile()
            profile['oidc']['scopes'] = scopes
            with self.assertRaises(g.Invalid):
                g.validate(profile)
        profile = self.profile()
        profile['oidc']['issuer'] = 'https://identity.example.com:8443/realms/dataslate'
        with self.assertRaises(g.Invalid):
            g.validate(profile)
        profile['network']['httpsEgressPorts'] = [8443]
        self.assertEqual(g.validate(profile)['networkPolicy']['httpsEgressPorts'], [8443])

    def test_global_local_metadata_and_supernet_egress_rejected(self):
        for cidr in ['0.0.0.0/0', '128.0.0.0/1', '0.0.0.0/1', '169.254.169.254/32', '127.0.0.1/32', '::/0', 'fe80::/10', 'fd00:ec2::254/128', '10.0.0.1/24']:
            with self.subTest(cidr=cidr):
                profile = self.profile()
                profile['network']['httpsEgressCidrs'] = [cidr]
                with self.assertRaises(g.Invalid):
                    g.validate(profile)

    def test_context_namespace_and_ports_reject_injection(self):
        for field in ['context', 'namespace', 'release']:
            profile = self.profile()
            profile[field] = 'x; echo unsafe'
            with self.assertRaises(g.Invalid):
                g.validate(profile)

        for value in [True, 0, 65536, '443']:
            profile = self.profile()
            profile['database']['port'] = value
            with self.assertRaises(g.Invalid):
                g.validate(profile)

    def test_system_namespaces_and_local_oidc_rejected(self):
        for namespace in ['default', 'kube-system', 'kube-public']:
            profile = self.profile()
            profile['namespace'] = namespace
            with self.assertRaises(g.Invalid):
                g.validate(profile)
        for issuer in ['https://localhost/realm', 'https://127.0.0.1/realm', 'https://169.254.169.254/realm', 'https://identity.example.com:0/realm']:
            profile = self.profile()
            profile['oidc']['issuer'] = issuer
            with self.assertRaises(g.Invalid):
                g.validate(profile)

    def test_migration_secret_cannot_double_as_runtime_ca(self):
        profile = self.profile()
        profile['oidc']['caSecret'] = dict(profile['database']['migrationSecret'])
        rendered = '''apiVersion: apps/v1
kind: Deployment
metadata: {name: dataslate-api}
spec:
  template:
    spec:
      volumes:
        - name: oidc-ca
          secret: {secretName: dataslate-migration-database}
---
apiVersion: batch/v1
kind: Job
metadata: {name: dataslate-migrate-1}
'''
        def render_fixture(*args, **kwargs):
            kwargs['stdout'].write(rendered.encode())
            return subprocess.CompletedProcess(args[0], 0)
        with patch.object(g.subprocess, 'run', render_fixture):
            with self.assertRaises(g.Invalid):
                g.render('helm', g.validate(profile), profile)

    def test_duplicate_json_and_size_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'profile.json'
            for data in ['{"schemaVersion":1,"schemaVersion":2}', ' '*65537, '{bad-json']:
                path.write_text(data)
                with self.assertRaises(g.Invalid):
                    g.load(path)

    def test_prerequisites_never_claim_qualification(self):
        prerequisites = g.prerequisites(self.profile())
        self.assertEqual(prerequisites['qualification'], 'not-performed')
        self.assertFalse(prerequisites['database']['credentialIdentityVerified'])
        self.assertTrue(all(set(ref) == {'name', 'keys', 'purpose'} for ref in prerequisites['existingSecrets']))

    def test_runbook_bootstrap_order_and_context(self):
        book = g.runbook(self.profile(), Path('/tmp/review bundle'), 'helm')
        install = book.index('helm install')
        migration = book.index('wait --for=condition=complete')
        grants = book.index('PGSERVICE=dataslate-migration psql')
        ready = book.index('rollout status deployment')
        self.assertLess(install, migration)
        self.assertLess(migration, grants)
        self.assertLess(grants, ready)
        self.assertIn('--format json >', book)
        self.assertNotIn('helm upgrade --install', book)
        self.assertIn("review bundle'/values.yaml", book)

    def test_runbook_retains_exact_tools_with_spaces(self):
        helm = '/opt/trusted tools/helm'
        python = '/opt/python environment/bin/python'
        output = Path('/tmp/review bundle')
        with patch.object(g.sys, 'executable', python):
            book = g.runbook(self.profile(), output, helm)
        commands = [line for line in book.splitlines() if line.startswith("'/opt/")]
        self.assertEqual(len(commands), 4)
        argv = [shlex.split(line) for line in commands]
        self.assertEqual(argv[0][:2], [helm, 'list'])
        self.assertEqual(argv[1][:2], [python, 'deploy/preflight/check.py'])
        self.assertEqual(argv[1][argv[1].index('--helm') + 1], helm)
        self.assertEqual(argv[1][argv[1].index('--values') + 1], str(output.resolve() / 'values.yaml'))
        self.assertEqual(argv[2][:3], [helm, 'install', 'dataslate'])
        self.assertEqual(argv[3], [python, 'deploy/scripts/verify-install.py', '--url', 'https://dataslate.example.com'])
        self.assertNotIn('\npython ', book)
        self.assertNotIn('\nhelm ', book)
        self.assertNotIn('--helm HELM', book)

    def test_helm_failure_does_not_echo_stderr(self):
        def failed(*args, **kwargs):
            kwargs['stderr'].write(b'password=do-not-disclose')
            return subprocess.CompletedProcess(args[0], 1)
        with patch.object(g.subprocess, 'run', failed):
            with self.assertRaises(g.Invalid) as error:
                g.render('helm', g.validate(self.profile()), self.profile())
            self.assertNotIn('do-not-disclose', str(error.exception))

    def test_helm_timeout_and_no_credential_environment(self):
        def timeout(argv, **kwargs):
            self.assertEqual(argv[1], 'template')
            self.assertEqual(kwargs['timeout'], 30)
            self.assertEqual(kwargs['env']['KUBECONFIG'], os.devnull)
            self.assertNotIn('AWS_ACCESS_KEY_ID', kwargs['env'])
            self.assertNotIn('--enable-dns', argv)
            raise subprocess.TimeoutExpired(argv, 30)
        with patch.dict(os.environ, {'AWS_ACCESS_KEY_ID': 'should-not-propagate'}), patch.object(g.subprocess, 'run', timeout):
            with self.assertRaises(g.Invalid):
                g.render('helm', g.validate(self.profile()), self.profile())

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(g, 'render') as renderer:
                with self.assertRaises(g.Invalid):
                    g.generate(HERE / 'profile.example.json', Path(tmp), 'helm')
                renderer.assert_not_called()


class ActualChartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helm = os.environ.get('HELM_BIN', 'helm')
        # Missing Helm must fail this suite; render evidence is not an optional skip.
        subprocess.run([cls.helm, 'version', '--short'], check=True, capture_output=True, timeout=15)

    def test_actual_chart_emits_review_files_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'reviewed'
            files = g.generate(HERE / 'profile.example.json', output, self.helm)
            self.assertEqual(set(files), {'values.yaml', 'namespace.yaml', 'prerequisites.json', 'RUNBOOK.md', 'rendered-core.yaml'})
            self.assertIn(shlex.quote(self.helm) + ' install dataslate ', files['RUNBOOK.md'])
            self.assertIn(shlex.quote(g.sys.executable) + ' deploy/preflight/check.py ', files['RUNBOOK.md'])
            docs = [d for d in yaml.safe_load_all(files['rendered-core.yaml']) if d]
            self.assertFalse(any(d['kind'] in ['Secret', 'Cluster', 'ClusterRole', 'CustomResourceDefinition'] for d in docs))
            job = next(d for d in docs if d['kind'] == 'Job')
            env = {v['name']: v for v in job['spec']['template']['spec']['containers'][0]['env']}
            self.assertEqual(env['DATASLATE_DATABASE_URL']['valueFrom']['secretKeyRef']['name'], 'dataslate-migration-database')
            api = next(d for d in docs if d['kind'] == 'Deployment' and d['metadata']['name'] == 'dataslate-api')
            self.assertNotIn('dataslate-migration-database', yaml.safe_dump(api))
            for file in output.iterdir():
                self.assertEqual(file.stat().st_mode & 0o777, 0o600)

    def test_actual_chart_optional_oidc_ca_scoped_api_only(self):
        profile = self.profile() if hasattr(self, 'profile') else copy.deepcopy(EXAMPLE)
        profile['oidc']['caSecret'] = {'name': 'identity-ca', 'key': 'root.pem'}
        docs = [d for d in yaml.safe_load_all(g.render(self.helm, g.validate(profile), profile)) if d]
        for doc in docs:
            if doc['kind'] in ['Job', 'Deployment']:
                text = yaml.safe_dump(doc)
                self.assertEqual('identity-ca' in text, doc['metadata']['name'] == 'dataslate-api')


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python3
"""Deterministic API fixtures; no kubeconfig, cluster or credentials required."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

import check


def args(directory):
    result = check.parser().parse_args(['--context', 'explicit-test-context', '--namespace', 'dataslate',
                                       '--chart', str(directory), '--format', 'json'])
    check.validate_arguments(result)
    return result


def crd():
    return {'metadata': {'name': 'sparkapplications.spark.apache.org'},
            'spec': {'group': 'spark.apache.org', 'scope': 'Namespaced', 'names': {'kind': 'SparkApplication'},
                     'versions': [{'name': 'v1', 'served': True, 'storage': True,
                                   'schema': {'openAPIV3Schema': {'properties': {'spec': {'properties': {
                                       'runtimeVersions': {'properties': {'sparkVersion': {'type': 'string'}}},
                                       'sparkConf': {'type': 'object'},
                                       'driverSpec': {'properties': {'podTemplateSpec': {'type': 'object'}}},
                                       'executorSpec': {'properties': {'podTemplateSpec': {'type': 'object'}}},
                                       'applicationTolerations': {'properties': {
                                           'resourceRetainPolicy': {'type': 'string'},
                                           'restartConfig': {'properties': {'restartPolicy': {'type': 'string'}}}}},
                                   }}}}}}]},
            'status': {'conditions': [{'type': 'Established', 'status': 'True'}]}}


MANIFEST = '''apiVersion: apps/v1
kind: Deployment
metadata: {name: dataslate-api}
spec:
  template:
    spec:
      containers:
      - name: api
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef: {name: db-credentials, key: database-url}
      volumes:
      - name: db-ca
        secret:
          secretName: db-ca
          items: [{key: ca.crt, path: ca.crt}]
'''


class Fixture:
    def __init__(self):
        self.commands = []
        self.overrides = {}
        self.render = MANIFEST

    def __call__(self, command, timeout):
        self.commands.append(command)
        if command[0] == 'helm':
            if 'render' in self.overrides:
                return self.overrides['render']
            return {'state': 'ok', 'text': self.render}
        assert command[1:3] == ['--context', 'explicit-test-context'], command
        assert command[3:5] == ['--request-timeout', '15s'], command
        query = command[5:]
        assert query[0] in ('get', 'version'), command
        if query[0] == 'version':
            return self.overrides.get('version', {'state': 'ok', 'text': json.dumps({'serverVersion': {'gitVersion': 'v1.35.0+k3s1'}})})
        resource = query[1]
        name = query[2] if len(query) > 2 and not query[2].startswith('-') else None
        if (resource, name) in self.overrides:
            return copy.deepcopy(self.overrides[(resource, name)])
        if resource == 'namespaces':
            return self.obj({'metadata': {'name': name, 'labels': {
                'pod-security.kubernetes.io/enforce': 'restricted',
                'pod-security.kubernetes.io/enforce-version': 'v1.34'}}, 'status': {'phase': 'Active'}})
        if resource == 'secrets':
            assert name in ('db-credentials', 'db-ca', 'db-app', 'optional', 'empty'), command
            assert query[-1] == 'go-template=' + check.SECRET_TEMPLATE, command
            return {'state': 'ok', 'text': 'KEYS\ndatabase-url\nca.crt\nuri\n'}
        if resource == 'customresourcedefinitions.apiextensions.k8s.io':
            return self.obj(crd()) if name == 'sparkapplications.spark.apache.org' else {'state': 'ok', 'text': ''}
        if resource in ('resourcequotas', 'limitranges', 'networkpolicies.networking.k8s.io') and name is None:
            return self.obj({'items': [{'metadata': {'name': 'fixture'}}]})
        if resource == '--raw':
            return self.obj({'resources': [{'name': 'networkpolicies', 'namespaced': True}]})
        return {'state': 'ok', 'text': ''}

    @staticmethod
    def obj(value):
        return {'state': 'ok', 'text': json.dumps(value)}


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.values = {'postgres': {'mode': 'external'}, 'networkPolicy': {'enabled': True},
                       'execution': {'enabled': False, 'namespaces': []}}
        self.fixture = Fixture()
        self.args = args(self.directory)

    def tearDown(self):
        self.temp.cleanup()

    def run_check(self):
        (self.directory / 'values.yaml').write_text(json.dumps(self.values))
        report = check.Preflight(self.args, self.fixture).run()
        self.by_id = {item['id']: item for item in report['checks']}
        return report

    def test_read_only_context_pinned_end_to_end_inventory(self):
        report = self.run_check()
        self.assertEqual(report['state'], 'incomplete')
        self.assertTrue(report['readOnly'])
        self.assertEqual(report['qualification'], 'not-performed')
        self.assertEqual(self.by_id['secret.dataslate.db-credentials']['status'], 'observed')
        self.assertEqual(self.by_id['ownership.dataslate.Deployment.dataslate-api']['status'], 'observed')
        forbidden = {'apply', 'delete', 'patch', 'create', 'install', 'upgrade', 'config', '--enable-dns', '--dependency-update', '--dry-run=server'}
        for command in self.fixture.commands:
            self.assertFalse(forbidden.intersection(command))
            if command[0] == 'kubectl' and 'secrets' in command:
                self.assertNotIn('json', command)
                self.assertNotIn('--all-namespaces', command)

    def test_explicit_old_cluster_blocks(self):
        self.fixture.overrides['version'] = self.fixture.obj({'serverVersion': {'gitVersion': 'v1.28.9'}})
        self.assertEqual(self.run_check()['state'], 'blocked')

    def test_version_failure_is_unknown_and_does_not_claim_missing(self):
        self.fixture.overrides['version'] = {'state': 'permission-denied'}
        self.run_check()
        self.assertEqual(self.by_id['cluster.version']['status'], 'unknown')

    def test_missing_namespace_blocks(self):
        self.fixture.overrides[('namespaces', 'dataslate')] = {'state': 'ok', 'text': ''}
        self.assertEqual(self.run_check()['state'], 'blocked')

    def test_terminating_namespace_blocks(self):
        self.fixture.overrides[('namespaces', 'dataslate')] = self.fixture.obj({'metadata': {'deletionTimestamp': '2026-01-01'}, 'status': {'phase': 'Active'}})
        self.run_check()
        self.assertEqual(self.by_id['namespace.dataslate']['status'], 'blocked')

    def test_existing_foreign_release_is_not_adopted(self):
        self.fixture.overrides[('deployments.apps', 'dataslate-api')] = {'state': 'ok', 'text': 'uid-123\nHelm\nother-team\nother-ns\n'}
        self.run_check()
        self.assertEqual(self.by_id['ownership.dataslate.Deployment.dataslate-api']['status'], 'blocked')
        self.assertFalse(any('patch' in c for c in self.fixture.commands))

    def test_existing_matching_release_is_observed_not_upgrade_qualified(self):
        self.fixture.overrides[('deployments.apps', 'dataslate-api')] = {'state': 'ok', 'text': 'uid-123\nHelm\ndataslate\ndataslate\n'}
        self.run_check()
        check_result = self.by_id['ownership.dataslate.Deployment.dataslate-api']
        self.assertEqual(check_result['status'], 'observed')
        self.assertIn('not an upgrade safety check', check_result['message'])

    def test_unowned_existing_object_blocks(self):
        self.fixture.overrides[('deployments.apps', 'dataslate-api')] = {'state': 'ok', 'text': 'uid-123\n<no value>\n<no value>\n<no value>\n'}
        self.run_check()
        self.assertEqual(self.by_id['ownership.dataslate.Deployment.dataslate-api']['status'], 'blocked')

    def test_permission_denied_secret_stays_unknown(self):
        self.fixture.overrides[('secrets', 'db-credentials')] = {'state': 'permission-denied'}
        self.run_check()
        self.assertEqual(self.by_id['secret.dataslate.db-credentials']['status'], 'unknown')

    def test_missing_secret_key_blocks_without_disclosing_other_keys(self):
        self.fixture.overrides[('secrets', 'db-credentials')] = {'state': 'ok', 'text': 'KEYS\nnot-needed-key\n'}
        report = self.run_check()
        self.assertEqual(self.by_id['secret.dataslate.db-credentials']['evidence']['missingKeys'], ['database-url'])
        self.assertNotIn('not-needed-key', json.dumps(report))

    def test_empty_existing_secret_is_missing_keys_not_absent(self):
        self.fixture.overrides[('secrets', 'db-credentials')] = {'state': 'ok', 'text': 'KEYS\n'}
        self.run_check()
        self.assertEqual(self.by_id['secret.dataslate.db-credentials']['message'], 'Required Secret keys are missing.')

    def test_secret_unexpected_value_output_is_rejected_without_echo(self):
        self.fixture.overrides[('secrets', 'db-credentials')] = {'state': 'ok', 'text': '{"data":{"password":"SUPER_SECRET_VALUE"}}'}
        report = self.run_check()
        self.assertNotIn('SUPER_SECRET_VALUE', json.dumps(report))
        self.assertNotIn('SUPER_SECRET_VALUE', check.render_text(report))
        self.assertEqual(self.by_id['secret.dataslate.db-credentials']['status'], 'unknown')

    def test_generated_cnpg_secret_is_pending_not_fabricated(self):
        self.fixture.render += '\n---\napiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata: {name: db}\n'
        self.fixture.overrides[('secrets', 'db-ca')] = {'state': 'ok', 'text': ''}
        self.run_check()
        self.assertEqual(self.by_id['secret.dataslate.db-ca']['status'], 'pending')

    def test_valid_apache_schema_does_not_imply_operator_readiness(self):
        self.values['execution'] = {'enabled': True, 'namespaces': ['compute']}
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.crd']['status'], 'observed')
        self.assertEqual(self.by_id['operator.spark.controller']['status'], 'unknown')
        self.assertEqual(self.by_id['compute.enforcement']['status'], 'unknown')

    def test_wrong_served_version_blocks(self):
        self.values['execution']['enabled'] = True
        obj = crd()
        obj['spec']['versions'][0]['name'] = 'v1beta1'
        self.fixture.overrides[('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.spark.apache.org')] = self.fixture.obj(obj)
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.crd']['status'], 'blocked')

    def test_missing_schema_field_blocks(self):
        self.values['execution']['enabled'] = True
        obj = crd()
        del obj['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']['driverSpec']
        self.fixture.overrides[('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.spark.apache.org')] = self.fixture.obj(obj)
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.crd']['status'], 'blocked')
        self.assertIn('spec.driverSpec.podTemplateSpec', self.by_id['operator.spark.crd']['evidence']['missingOrIncompatibleFields'])

    def test_unestablished_crd_blocks(self):
        self.values['execution']['enabled'] = True
        obj = crd()
        obj['status']['conditions'][0]['status'] = 'False'
        self.fixture.overrides[('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.spark.apache.org')] = self.fixture.obj(obj)
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.crd']['status'], 'blocked')

    def test_kubeflow_is_never_assumed_execution_compatible(self):
        self.values['execution']['enabled'] = True
        self.fixture.overrides[('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.spark.apache.org')] = {'state': 'ok', 'text': ''}
        self.fixture.overrides[('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.sparkoperator.k8s.io')] = self.fixture.obj({'metadata': {'name': 'sparkapplications.sparkoperator.k8s.io'}})
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.crd']['status'], 'blocked')
        self.assertEqual(self.by_id['operator.kubeflow']['status'], 'unsupported')

    def test_operator_explicit_location_and_available_generation(self):
        self.values['execution']['enabled'] = True
        self.args.spark_operator = 'operators/spark'
        self.fixture.overrides[('deployments.apps', 'spark')] = {'state': 'ok', 'text': 'uid-123\nHelm\nexisting-spark\noperators\n3\n3\n2\n2\nregistry.example/spark-operator:1.0.0\n'}
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.controller']['status'], 'observed')
        self.assertEqual(self.by_id['operator.spark.controller']['evidence']['owner']['release'], 'existing-spark')

    def test_stale_operator_observed_generation_blocks(self):
        self.values['execution']['enabled'] = True
        self.args.spark_operator = 'operators/spark'
        self.fixture.overrides[('deployments.apps', 'spark')] = {'state': 'ok', 'text': 'uid-123\nHelm\nexisting-spark\noperators\n3\n2\n2\n2\nregistry.example/spark-operator:1.0.0\n'}
        self.run_check()
        self.assertEqual(self.by_id['operator.spark.controller']['status'], 'blocked')

    def test_compute_boundary_missing_quota_blocks(self):
        self.values['execution'] = {'enabled': True, 'namespaces': ['compute']}
        self.fixture.overrides[('resourcequotas', None)] = self.fixture.obj({'items': []})
        self.run_check()
        self.assertEqual(self.by_id['namespace.compute.resourcequotas']['status'], 'blocked')

    def test_unpinned_pod_security_is_not_accepted(self):
        self.values['execution'] = {'enabled': True, 'namespaces': ['compute']}
        self.fixture.overrides[('namespaces', 'compute')] = self.fixture.obj({'metadata': {'labels': {'pod-security.kubernetes.io/enforce': 'restricted', 'pod-security.kubernetes.io/enforce-version': 'latest'}}, 'status': {'phase': 'Active'}})
        self.run_check()
        self.assertEqual(self.by_id['namespace.compute.pod-security']['status'], 'blocked')

    def test_missing_ingress_class_blocks(self):
        self.values['ingress'] = {'enabled': True, 'className': 'alb'}
        self.run_check()
        self.assertEqual(self.by_id['ingress.class']['status'], 'blocked')

    def test_network_api_does_not_prove_enforcement(self):
        self.run_check()
        self.assertEqual(self.by_id['network.api']['status'], 'observed')
        self.assertEqual(self.by_id['network.enforcement']['status'], 'unknown')

    def test_render_failure_stops_before_cluster_and_hides_error(self):
        self.fixture.overrides['render'] = {'state': 'command-failed', 'text': 'secret-password-do-not-print'}
        report = self.run_check()
        self.assertEqual(report['state'], 'blocked')
        self.assertEqual(len(self.fixture.commands), 1)
        self.assertNotIn('secret-password-do-not-print', json.dumps(report))

    def test_values_changed_during_render_abort_before_cluster(self):
        (self.directory / 'values.yaml').write_text(json.dumps(self.values))

        def mutate(command, timeout):
            self.assertEqual(command[0], 'helm')
            (self.directory / 'values.yaml').write_text('postgres: {mode: managed}')
            return {'state': 'ok', 'text': MANIFEST}

        with self.assertRaises(check.InvalidInput):
            check.Preflight(self.args, mutate).run()

    def test_total_budget_exhaustion_yields_unknown_not_ready(self):
        (self.directory / 'values.yaml').write_text(json.dumps(self.values))
        preflight = check.Preflight(self.args, self.fixture)
        preflight.calls = 300
        result = preflight.kubectl(['version', '-o', 'json'])
        self.assertEqual(result['state'], 'budget-exhausted')
        self.assertFalse(self.fixture.commands)

    def test_layering_replaces_arrays_and_rejects_duplicate_keys(self):
        self.assertEqual(check.merge({'x': {'a': 1, 'b': 2}, 'list': [1]}, {'x': {'a': None}, 'list': [2]}), {'x': {'b': 2}, 'list': [2]})
        with self.assertRaises(check.InvalidInput):
            check.documents(b'a: 1\na: 2\n')

    def test_yaml_recursive_alias_and_unsafe_tag_rejected(self):
        for raw in (b'a: &a [*a]', b'!!python/object:sys.stdout {}', b'a: 2026-11-08'):
            with self.subTest(raw=raw), self.assertRaises(check.InvalidInput):
                check.documents(raw)

    def test_input_bounds(self):
        with self.assertRaises(check.InvalidInput):
            check.documents(b'x' * (check.MAX_BYTES + 1))
        self.args.context = 'context\nUNTRUSTED'
        with self.assertRaises(check.InvalidInput):
            check.validate_arguments(self.args)

    def test_projected_secret_ingress_and_pull_secret_reference_dedup(self):
        obj = {'kind': 'Deployment', 'metadata': {'name': 'api'}, 'spec': {'template': {'spec': {
            'imagePullSecrets': [{'name': 'pull'}], 'volumes': [{'projected': {'sources': [{'secret': {'name': 'gateway', 'items': [{'key': 'token'}]}}]}}],
            'containers': [{'env': [{'valueFrom': {'secretKeyRef': {'name': 'gateway', 'key': 'token'}}}]}]}}}}
        ingress = {'kind': 'Ingress', 'spec': {'tls': [{'secretName': 'tls'}]}}
        refs = check.secret_references([obj, ingress], 'dataslate')
        self.assertEqual(refs[('dataslate', 'gateway')]['keys'], {'token'})
        self.assertEqual(refs[('dataslate', 'tls')]['keys'], {'tls.crt', 'tls.key'})
        self.assertEqual(refs[('dataslate', 'pull')]['keys'], set())

    def test_rendered_kind_control_characters_cannot_enter_report(self):
        self.fixture.render = 'apiVersion: v1\nkind: "Bad\\nSECRET_VALUE"\nmetadata: {name: fixture}\n'
        with self.assertRaises(check.InvalidInput):
            self.run_check()

    def test_subprocess_stderr_is_categorized_without_disclosure(self):
        result = check.execute([sys.executable, '-c', 'import sys; sys.stderr.write("Error from server (Forbidden): secret=DO_NOT_LEAK"); sys.exit(1)'])
        self.assertEqual(result, {'state': 'permission-denied'})

    def test_subprocess_output_limit_and_timeout(self):
        result = check.execute([sys.executable, '-c', f'print("x" * {check.MAX_BYTES + 1})'])
        self.assertEqual(result['state'], 'output-limit')
        result = check.execute([sys.executable, '-c', 'import time; time.sleep(2)'], timeout=0.1)
        self.assertEqual(result['state'], 'timeout')

    def test_invalid_cli_input_never_echoes_yaml_values(self):
        (self.directory / 'values.yaml').write_text('bad: !!python/object:SECRET_VALUE {}')
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = check.main(['--context', 'test', '--namespace', 'dataslate', '--chart', str(self.directory)])
        self.assertEqual(code, 2)
        self.assertNotIn('SECRET_VALUE', stdout.getvalue() + stderr.getvalue())

    def test_go_templates_have_escaped_not_literal_line_breaks(self):
        for template in (check.META_TEMPLATE, check.SECRET_TEMPLATE, check.DEPLOYMENT_TEMPLATE):
            self.assertNotIn('\n', template)
            self.assertIn(r'\n', template)

    @unittest.skipUnless(os.environ.get('DATASLATE_PREFLIGHT_HELM'), 'Set DATASLATE_PREFLIGHT_HELM for real local Helm render checks')
    def test_real_chart_render_uses_manifest_derived_secrets(self):
        self.args.chart = check.ROOT / 'deploy/helm/dataslate'
        self.args.values = [check.ROOT / 'deploy/examples/eks-external.yaml']
        self.args.helm = os.environ['DATASLATE_PREFLIGHT_HELM']
        secret_names = []

        def local_only(command, timeout):
            if command[0] == self.args.helm:
                return check.execute(command, timeout)
            if command[5:7] == ['get', 'secrets']:
                self.assertEqual(command[-1], 'go-template=' + check.SECRET_TEMPLATE)
                secret_names.append(command[7])
                return {'state': 'ok', 'text': 'KEYS\ndatabase-url\nca.crt\n'}
            return self.fixture(command, timeout)

        report = check.Preflight(self.args, local_only).run()
        self.assertEqual(report['checks'][0]['status'], 'observed')
        self.assertEqual(set(secret_names), {'dataslate-database', 'dataslate-database-ca'})
        self.assertNotIn('registry.example.com', json.dumps(report))

    @unittest.skipUnless(os.environ.get('DATASLATE_PREFLIGHT_HELM'), 'Set DATASLATE_PREFLIGHT_HELM for native Go-template validation')
    def test_native_go_template_never_prints_secret_values(self):
        # Helm uses Go templates too; this exercises the actual formatting expression
        # without starting kubectl or needing any Kubernetes discovery/credential.
        (self.directory / 'Chart.yaml').write_text('apiVersion: v2\nname: template-check\nversion: 0.1.0\n')
        (self.directory / 'templates').mkdir()
        template = '{{- define "keys" -}}' + check.SECRET_TEMPLATE + '{{- end -}}\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\ndata:\n  observed: |\n{{ include "keys" (dict "data" (dict "api-key" "NEVER_PRINT_THE_SECRET")) | indent 4 }}\n'
        (self.directory / 'templates/probe.yaml').write_text(template)
        result = check.execute([os.environ['DATASLATE_PREFLIGHT_HELM'], 'template', 'test', str(self.directory)])
        self.assertEqual(result['state'], 'ok')
        self.assertNotIn('NEVER_PRINT_THE_SECRET', result['text'])
        obj = check.documents(result['text'].encode())[0]
        self.assertEqual(obj['data']['observed'], 'KEYS\napi-key\n')


if __name__ == '__main__':
    unittest.main()

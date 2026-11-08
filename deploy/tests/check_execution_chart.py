"""Render opt-in execution and check the actual privilege/network boundary."""
import copy
import json
from pathlib import Path
import tempfile

import yaml


def check_execution_chart(run, external_args):
    binding = {
        'id': 'approved', 'workspaceId': 'default', 'name': 'Approved application',
        'operator': 'apache', 'image': 'registry.example.com/spark@sha256:' + 'a' * 64,
        'mainApplicationFile': 'local:///opt/jobs/job.py', 'namespace': 'dataslate-compute',
        'serviceAccount': 'engineering-driver', 'executorInstances': 1,
        'allowedSubjects': [{'issuer': 'https://identity.example.com/realms/dataslate', 'subject': 'engineer-123'}],
    }
    values = {'execution': {'enabled': True, 'operatorAccepted': True, 'runtimePolicyAccepted': True,
                            'kubernetesEgressCidrs': ['10.0.0.1/32'], 'namespaces': ['dataslate-compute'], 'bindings': [binding]}}
    with tempfile.TemporaryDirectory(prefix='dataslate-chart-execution-') as directory:
        path = Path(directory) / 'execution.json'
        path.write_text(json.dumps(values))
        args = [*external_args, '-f', path, '--api-versions', 'spark.apache.org/v1/SparkApplication']
        docs = [d for d in yaml.safe_load_all(run(args)) if d]
        assert not any(d['kind'] in ('ClusterRole', 'ClusterRoleBinding', 'CustomResourceDefinition', 'Namespace', 'Secret') for d in docs)
        roles = [d for d in docs if d['kind'] == 'Role']
        assert len(roles) == 2
        for role in roles:
            assert role['metadata']['namespace'] == 'dataslate-compute'
            spark = role['rules'][0]
            if role['metadata']['name'].endswith('-controller'):
                assert len(role['rules']) == 3
                assert role['rules'][1] == {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get', 'create', 'patch', 'delete']}
                assert role['rules'][2] == {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']}
            else:
                _, pods, logs = role['rules']
                assert pods == {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']}
                assert logs == {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}
            assert spark['apiGroups'] == ['spark.apache.org'] and spark['resources'] == ['sparkapplications']
            assert spark['verbs'] == (['get', 'create', 'delete'] if role['metadata']['name'].endswith('-controller') else ['get'])
        for b in [d for d in docs if d['kind'] == 'RoleBinding']:
            assert b['metadata']['namespace'] == 'dataslate-compute'
            assert b['subjects'] == [{'kind': 'ServiceAccount', 'name': b['metadata']['name'], 'namespace': 'dataslate'}]
        config = next(d for d in docs if d['kind'] == 'ConfigMap' and d['metadata']['name'] == 'dataslate-execution')
        assert json.loads(config['data']['bindings.json']) == [binding]
        for d in [d for d in docs if d['kind'] == 'Deployment']:
            pod = d['spec']['template']['spec'];component = d['metadata']['name'].removeprefix('dataslate-')
            assert pod['automountServiceAccountToken'] == (component in ('api', 'controller'))
            if component == 'web': continue
            c = pod['containers'][0]; env = {e['name']: e for e in c['env']}
            assert env['DATASLATE_EXECUTION_NAMESPACES']['value'] == 'dataslate-compute'
            assert env['PGSSLMODE']['value'] == 'verify-full'
            assert c['securityContext']['capabilities']['drop'] == ['ALL']
            if component == 'api':
                assert env['DATASLATE_EXECUTION_BINDINGS_FILE']['value'] == '/etc/dataslate/execution/bindings.json'
                assert any(v.get('configMap', {}).get('name') == 'dataslate-execution' for v in pod['volumes'])
            else:
                assert any(v.get('configMap', {}).get('name') == 'dataslate-execution' for v in pod['volumes'])
                assert env['DATASLATE_EXECUTION_BINDINGS_FILE']['value'] == '/etc/dataslate/execution/bindings.json'
                assert all(c[p]['httpGet']['port'] == 'http' for p in ('startupProbe', 'livenessProbe', 'readinessProbe'))
                assert c['ports'][0]['containerPort'] == 8081
        controller = next(d for d in docs if d['kind'] == 'NetworkPolicy' and d['metadata']['name'] == 'dataslate-controller')
        assert controller['spec']['ingress'] == []
        assert controller['spec']['egress'][1] == {'to': [{'ipBlock': {'cidr': '10.0.0.1/32'}}], 'ports': [{'protocol': 'TCP', 'port': 443}]}
        assert '0.0.0.0/0' not in json.dumps(controller)
        negatives = [
            (['--set', 'execution.operatorAccepted=false'], 'operatorAccepted'),
            (['--set', 'execution.runtimePolicyAccepted=false'], 'runtimePolicyAccepted'),
            (['--set', 'execution.bindings[0].maxRunSeconds=59'], 'maxRunSeconds'),
            (['--set', 'execution.bindings[0].maxRunSeconds=86401'], 'maxRunSeconds'),
            (['--set-json', 'execution.namespaces=[]'], 'explicit namespace allowlist'),
            (['--set-json', 'execution.kubernetesEgressCidrs=[]'], 'Kubernetes API egress'),
            (['--set', 'execution.enabled=false'], 'bindings require'),
            (['--set', 'execution.bindings[0].image=registry.example.com/spark:latest'], 'image'),
            (['--set', 'execution.bindings[0].operator=kubeflow'], 'apache'),
            (['--set', 'execution.namespaces[0]=dataslate'], 'dedicated namespaces'),
            (['--set', 'execution.namespaces[0]=kube-system'], 'dedicated namespaces'),
            (['--set', 'execution.namespaces[0]=default'], 'dedicated namespaces'),
            (['--set', 'execution.bindings[0].mainApplicationFile=s3a://bucket/job.py'], 'mainApplicationFile'),
            (['--set', 'execution.bindings[0].allowedSubjects[0].issuer=https://wrong.example.com'], 'issuers must match'),
            (['--set', 'execution.bindings[0].namespace=unapproved'], 'outside execution.namespaces'),
        ]
        for flags, message in negatives:
            run([*args, *flags], message)
        run([*external_args, '-f', path], 'SparkApplication CRD')
        duplicate = copy.deepcopy(values); duplicate['execution']['bindings'].append(binding)
        path.write_text(json.dumps(duplicate)); run(args, 'IDs must be unique')
        notebook = copy.deepcopy(values)
        notebook['execution']['bindings'][0].update(executionKind='notebook-python-batch', mainApplicationFile='')
        path.write_text(json.dumps(notebook)); run(args)
        del notebook['execution']['bindings'][0]['mainApplicationFile']
        path.write_text(json.dumps(notebook)); run(args)
        run([*args, '--set', 'execution.bindings[0].mainApplicationFile=local:///opt/override.py'], 'mainApplicationFile')
        run([*args, '--set', 'execution.bindings[0].executionKind=interactive-python'], 'executionKind')
        ingestion = copy.deepcopy(notebook)
        ingestion_binding = ingestion['execution']['bindings'][0]
        ingestion_binding.update(executionKind='file-ingestion-batch', ingestionStorage={
            'readPrefixes': ['s3://example-lake/incoming/'],
            'writePrefixes': ['s3://example-lake/managed/', 's3://example-lake/checkpoints/'],
        })
        path.write_text(json.dumps(ingestion)); run(args)
        ingestion_negatives = [
            (['--set-json', 'execution.bindings[0].ingestionStorage=null'], 'ingestionStorage'),
            (['--set-json', 'execution.bindings[0].ingestionStorage.readPrefixes=[]'], 'readPrefixes'),
            (['--set-json', 'execution.bindings[0].ingestionStorage.writePrefixes=[]'], 'writePrefixes'),
            (['--set', 'execution.bindings[0].ingestionStorage.readPrefixes[0]=https://example.com/source/'], 'readPrefixes'),
            (['--set', 'execution.bindings[0].ingestionStorage.readPrefixes[0]=s3://example-lake/incoming/../other/'], 'readPrefixes'),
            (['--set', 'execution.bindings[0].ingestionStorage.writePrefixes[0]=s3://example-lake/data'], 'writePrefixes'),
            (['--set', 'execution.bindings[0].executionKind=notebook-python-batch'], "'not' failed"),
            (['--set', 'execution.bindings[0].ingestionStorage.readPrefixes[0]=s3://example-lake/'], 'readPrefixes'),
            (['--set', 'execution.bindings[0].ingestionStorage.readPrefixes[0]=s3://127.0.0.1/data/'], 'readPrefixes'),
            (['--set', 'execution.bindings[0].ingestionStorage.readPrefixes[0]=s3://example..lake/data/'], 'readPrefixes'),
        ]
        for flags, expected in ingestion_negatives:
            run([*args, *flags], expected)
        # A second namespace must receive its own roles without broadening existing roles.
        second = copy.deepcopy(binding); second.update(id='another', namespace='analytics-compute')
        duplicate['execution']['bindings'][1] = second; duplicate['execution']['namespaces'].append('analytics-compute'); path.write_text(json.dumps(duplicate))
        docs = [d for d in yaml.safe_load_all(run(args)) if d]
        roles = [d for d in docs if d['kind'] == 'Role']
        assert len(roles) == 4 and {d['metadata']['namespace'] for d in roles} == {'dataslate-compute', 'analytics-compute'}
        # Revoking every grant must preserve the controller and its cancellation privileges.
        for flags in (['--set-json', 'execution.bindings=[]'], ['--set-json', 'execution.bindings[0].allowedSubjects=[]']):
            drain = [d for d in yaml.safe_load_all(run([*args, *flags])) if d]
            assert len([d for d in drain if d['kind'] == 'Role']) == 4
            assert any(d['kind'] == 'Deployment' and d['metadata']['name'] == 'dataslate-controller' for d in drain)
    print('PASS execution opt-in: namespaced RBAC, pinned bindings, controller isolation and 27 unsafe configurations rejected')

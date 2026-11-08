#!/usr/bin/env python3
"""Probe notebook source admission in an explicit disposable LOCAL cluster.

Creates one reserved, empty fixture ConfigMap and deletes it in finally. Identity
impersonation with system:masters isolates admission checks from RBAC; the Go
execution harness separately uses real minimally privileged service-account tokens.
"""
import argparse
import json
from pathlib import Path
import subprocess
import uuid


def verify(config, context, kubectl):
    namespace = config['namespace']
    identity = config['notebookSources']
    controller = f"system:serviceaccount:{identity['controllerNamespace']}:{identity['controllerServiceAccount']}"
    driver = f"system:serviceaccount:{namespace}:{config['serviceAccount']}"
    policy_name = 'dataslate-' + namespace + '-sources'
    base = [kubectl, '--context', context, '-n', namespace]
    checks = []

    def command(args, body=None, user=None):
        auth = ['--as', user, '--as-group', 'system:masters'] if user else []
        return subprocess.run(base + auth + args, input=None if body is None else json.dumps(body), text=True, capture_output=True, timeout=30)

    def check(name, args, body=None, user=None, allowed=True, immutable=False):
        result = command(args, body, user)
        passed = result.returncode == 0 if allowed else result.returncode != 0 and ('field is immutable' if immutable else 'ValidatingAdmissionPolicy') in result.stderr
        checks.append({'name': name, 'passed': passed, 'expected': 'allow' if allowed else 'deny', 'diagnostic': result.stderr[:3000]})
        if not passed:
            raise RuntimeError(f'{name}: {result.stderr}')
        return result

    result = command(['get', 'validatingadmissionpolicy', policy_name, '-o', 'json'])
    if result.returncode:
        raise RuntimeError(result.stderr)
    policy = json.loads(result.stdout)
    status = policy.get('status', {})
    if status.get('observedGeneration') != policy['metadata']['generation'] or status.get('typeChecking', {}).get('expressionWarnings'):
        raise RuntimeError('Source policy is not observed or has CEL type warnings')
    result = command(['get', 'validatingadmissionpolicybinding', policy_name, '-o', 'json'])
    binding = json.loads(result.stdout)['spec']
    if binding.get('policyName') != policy_name or binding.get('validationActions') != ['Deny'] or binding.get('matchResources', {}).get('namespaceSelector', {}).get('matchLabels') != {'kubernetes.io/metadata.name': namespace}:
        raise RuntimeError('Unexpected source policy binding')

    name = 'ds-' + uuid.uuid4().hex + '-source'
    fixture = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': name, 'namespace': namespace}, 'immutable': True, 'data': {'source.py': '# empty local admission fixture\n'}}
    dry_create = ['create', '--dry-run=server', '-f', '-', '-o', 'json']
    check('controller-create-unlabeled-source', dry_create, fixture, controller)
    check('driver-create-unlabeled-source', dry_create, fixture, driver, False)
    ordinary = dict(fixture, metadata={'name': 'spark-local-source-probe', 'namespace': namespace})
    check('driver-create-ordinary-configmap', dry_create, ordinary, driver)
    created = False
    try:
        result = command(['create', '-f', '-', '-o', 'json'], fixture, controller)
        if result.returncode:
            raise RuntimeError(result.stderr)
        created = True
        annotation = ['patch', 'configmap', name, '--dry-run=server', '--type=merge', '-p', json.dumps({'metadata': {'annotations': {'qualification': 'probe'}}}), '-o', 'json']
        check('controller-update-source-annotation', annotation, user=controller)
        check('driver-update-source-annotation', annotation, user=driver, allowed=False)
        data = ['patch', 'configmap', name, '--dry-run=server', '--type=merge', '-p', json.dumps({'data': {'source.py': '# changed'}}), '-o', 'json']
        check('controller-cannot-mutate-immutable-source', data, user=controller, allowed=False, immutable=True)
        check('driver-delete-source', ['delete', 'configmap', name, '--dry-run=server'], user=driver, allowed=False)
        check('controller-delete-source', ['delete', 'configmap', name, '--dry-run=server'], user=controller)
    finally:
        if created:
            result = command(['delete', 'configmap', name], user=controller)
            if result.returncode:
                raise RuntimeError('Fixture cleanup failed: ' + result.stderr)
    return {'passed': all(c['passed'] for c in checks), 'scope': 'local API server admission; eight probes, one empty fixture ConfigMap created and removed; elevated impersonation isolates admission from RBAC', 'context': context, 'namespace': namespace, 'policyGeneration': policy['metadata']['generation'], 'checks': checks}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--context', required=True)
    parser.add_argument('--kubectl', default='kubectl')
    args = parser.parse_args()
    print(json.dumps(verify(json.loads(Path(args.config).read_text()), args.context, args.kubectl), indent=2))

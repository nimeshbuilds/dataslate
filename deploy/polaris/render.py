#!/usr/bin/env python3
"""Render the pinned Polaris chart and an explicit bootstrap Job; never apply it.

Prerequisites are operator-owned Secrets, PostgreSQL schema and an IAM-bound
ServiceAccount. The generated release owns only new Polaris namespaced resources.
"""
import argparse
import copy
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import tempfile
import yaml

ROOT = Path(__file__).resolve().parent


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,48}[a-z0-9]', value):
        raise ValueError('Expected a 2–50 character Kubernetes name')
    return value


def settings(config):
    keys = {'name', 'namespace', 'realm', 'replicas', 'serviceAccount', 'databaseSecret', 'databaseSchema', 'tlsSecret', 'tokenKeySecret', 'rootCredentialsSecret', 'clientNamespace', 'clientLabels', 'egress'}
    if not isinstance(config, dict) or set(config) != keys:
        raise ValueError('Missing or unknown managed profile fields')
    for key in ('name', 'namespace', 'serviceAccount', 'databaseSecret', 'tlsSecret', 'tokenKeySecret', 'rootCredentialsSecret', 'clientNamespace'):
        name(config[key])
    if not isinstance(config['realm'], str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', config['realm']):
        raise ValueError('Invalid realm')
    if not isinstance(config['databaseSchema'], str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', config['databaseSchema']):
        raise ValueError('Invalid database schema')
    if type(config['replicas']) is not int or not 1 <= config['replicas'] <= 16:
        raise ValueError('replicas must be 1..16')
    labels = config['clientLabels']
    if not isinstance(labels, dict) or not labels or any(not isinstance(k, str) or not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9_./-]{1,253}', k) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', v) for k, v in labels.items()):
        raise ValueError('Explicit client labels required')
    if not isinstance(config['egress'], list) or not 1 <= len(config['egress']) <= 100:
        raise ValueError('Explicit bounded database/storage egress required')
    for e in config['egress']:
        if not isinstance(e, dict) or set(e) != {'cidr', 'port'} or type(e['port']) is not int or not 1 <= e['port'] <= 65535:
            raise ValueError('Invalid egress rule')
        subnet = ipaddress.ip_network(e['cidr'], strict=True)
        if subnet.prefixlen == 0 or subnet.is_multicast or subnet.is_loopback or subnet.is_link_local:
            raise ValueError('Unrestricted or local egress is not accepted')
    image = json.loads((ROOT / 'upstream-lock.json').read_text())['image']
    return {
        'fullnameOverride': config['name'], 'replicaCount': config['replicas'],
        'image': {'repository': 'apache/polaris', 'tag': image.split(':', 1)[1]},
        'serviceAccount': {'create': False, 'name': config['serviceAccount']},
        'podLabels': {'dataslate.io/polaris-release': config['name']},
        'realmContext': {'type': 'default', 'realms': [config['realm']], 'requireHeader': True},
        'persistence': {'type': 'relational-jdbc', 'relationalJdbc': {'additionalProperties': {'currentSchema': config['databaseSchema']}, 'secret': {'name': config['databaseSecret'], 'username': 'username', 'password': 'password', 'jdbcUrl': 'jdbcUrl'}}},
        'authentication': {'type': 'internal', 'tokenBroker': {'type': 'rsa-key-pair', 'maxTokenGeneration': 'PT1H', 'secret': {'name': config['tokenKeySecret'], 'rsaKeyPair': {'publicKey': 'public.pem', 'privateKey': 'private.pem'}}}},
        'features': {'SUPPORTED_CATALOG_STORAGE_TYPES': ['S3', 'GCS', 'AZURE']},
        'service': {'type': 'ClusterIP', 'ports': [{'name': 'polaris-https', 'port': 8443, 'targetPort': 8443, 'protocol': 'TCP'}]},
        'resources': {'requests': {'cpu': '250m', 'memory': '768Mi'}, 'limits': {'cpu': '1', 'memory': '1Gi'}},
        'containerSecurityContext': {'allowPrivilegeEscalation': False, 'runAsNonRoot': True, 'runAsUser': 10000, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}},
        'extraEnv': [{'name': 'JAVA_TOOL_OPTIONS', 'value': '-Xms128m -Xmx512m'}, {'name': 'AWS_EC2_METADATA_DISABLED', 'value': 'true'}],
        'extraVolumes': [{'name': 'server-tls', 'secret': {'secretName': config['tlsSecret']}}],
        'extraVolumeMounts': [{'name': 'server-tls', 'mountPath': '/etc/polaris-tls', 'readOnly': True}],
        'advancedConfig': {'quarkus.http.ssl-port': '8443', 'quarkus.http.insecure-requests': 'disabled', 'quarkus.http.ssl.certificate.files': '/etc/polaris-tls/tls.crt', 'quarkus.http.ssl.certificate.key-files': '/etc/polaris-tls/tls.key'},
        'podDisruptionBudget': {'enabled': config['replicas'] > 1, 'minAvailable': 1},
        'terminationGracePeriodSeconds': 30,
        'logging': {'console': {'json': True}, 'file': {'enabled': False}},
    }


def extras(config):
    labels = {'dataslate.io/polaris-release': config['name']}
    meta = {'namespace': config['namespace'], 'labels': labels}
    env = [{'name': 'polaris.persistence.type', 'value': 'relational-jdbc'}, {'name': 'quarkus.datasource.jdbc.additional-jdbc-properties.currentSchema', 'value': config['databaseSchema']}, {'name': 'JAVA_TOOL_OPTIONS', 'value': '-Xms64m -Xmx384m'}]
    for suffix, key in [('username', 'username'), ('password', 'password'), ('jdbc.url', 'jdbcUrl')]:
        env.append({'name': 'quarkus.datasource.' + suffix, 'valueFrom': {'secretKeyRef': {'name': config['databaseSecret'], 'key': key}}})
    security = {'runAsNonRoot': True, 'runAsUser': 10000, 'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {**meta, 'name': config['name'] + '-bootstrap'}, 'spec': {
        'backoffLimit': 0, 'activeDeadlineSeconds': 180, 'template': {'metadata': {'labels': labels}, 'spec': {
            'automountServiceAccountToken': False, 'restartPolicy': 'Never',
            'securityContext': {'fsGroup': 10001, 'seccompProfile': {'type': 'RuntimeDefault'}},
            'containers': [{'name': 'bootstrap', 'image': json.loads((ROOT / 'upstream-lock.json').read_text())['adminImage'], 'args': ['bootstrap', '--credentials-file=/bootstrap/credentials.json'], 'env': env, 'securityContext': security,
                'resources': {'requests': {'cpu': '100m', 'memory': '256Mi'}, 'limits': {'cpu': '1', 'memory': '512Mi'}},
                'volumeMounts': [{'name': 'credentials', 'mountPath': '/bootstrap', 'readOnly': True}, {'name': 'temp', 'mountPath': '/tmp'}]}],
            'volumes': [{'name': 'credentials', 'secret': {'secretName': config['rootCredentialsSecret'], 'defaultMode': 288, 'items': [{'key': 'credentials.json', 'path': 'credentials.json'}]}}, {'name': 'temp', 'emptyDir': {'sizeLimit': '128Mi'}}],
        }}}}
    dns = {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}}, 'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}}}], 'ports': [{'protocol': 'UDP', 'port': 53}, {'protocol': 'TCP', 'port': 53}]}
    policy = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': {**meta, 'name': config['name']}, 'spec': {
        'podSelector': {'matchLabels': labels}, 'policyTypes': ['Ingress', 'Egress'],
        'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': config['clientNamespace']}}, 'podSelector': {'matchLabels': config['clientLabels']}}], 'ports': [{'protocol': 'TCP', 'port': 8443}]}],
        'egress': [dns] + [{'to': [{'ipBlock': {'cidr': e['cidr']}}], 'ports': [{'protocol': 'TCP', 'port': e['port']}]} for e in config['egress']],
    }}
    return [policy, job]


def adapt(documents, config):
    documents = copy.deepcopy(documents)
    seen = {'Deployment': 0, 'ConfigMap': 0}
    for d in documents:
        if not d: continue
        if d.get('metadata', {}).get('namespace') != config['namespace']:
            raise ValueError('Unexpected namespace from upstream chart')
        if d['kind'] in ('ClusterRole', 'ClusterRoleBinding', 'CustomResourceDefinition', 'Secret', 'ServiceAccount'):
            raise ValueError('Upstream chart tried to own an external or cluster resource')
        d['metadata'].setdefault('annotations', {})['dataslate.io/upstream-profile'] = 'polaris-1.8.0'
        if d['kind'] == 'Deployment':
            seen['Deployment'] += 1
            p = d['spec']['template']['spec']; p['automountServiceAccountToken'] = False
            if len(p['containers']) != 1: raise ValueError('Unexpected Polaris containers')
            if p['containers'][0]['image'] != json.loads((ROOT / 'upstream-lock.json').read_text())['image']:
                raise ValueError('Unexpected Polaris image')
        if d['kind'] == 'ConfigMap' and 'application.properties' in d.get('data', {}):
            seen['ConfigMap'] += 1
            if 'quarkus.http.insecure-requests=disabled' not in d['data']['application.properties']:
                raise ValueError('TLS settings missing from chart')
    if seen != {'Deployment': 1, 'ConfigMap': 1}: raise ValueError('Unexpected pinned chart shape')
    return documents + extras(config)


def render(config, helm, chart):
    values = settings(config)
    lock = json.loads((ROOT / 'upstream-lock.json').read_text())
    if hashlib.sha256(Path(chart).read_bytes()).hexdigest() != lock['sha256']:
        raise ValueError('Upstream chart checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='dataslate-polaris-render-') as directory:
        path = Path(directory) / 'values.json'; path.write_text(json.dumps(values))
        output = subprocess.run([helm, 'template', config['name'], str(chart), '--namespace', config['namespace'], '--skip-tests', '-f', str(path)], capture_output=True, text=True, check=True).stdout
    return adapt([d for d in yaml.safe_load_all(output) if d], config)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True); parser.add_argument('--helm', required=True); parser.add_argument('--chart', required=True)
    args = parser.parse_args()
    print(yaml.safe_dump_all(render(json.loads(Path(args.config).read_text()), args.helm, args.chart), sort_keys=False))

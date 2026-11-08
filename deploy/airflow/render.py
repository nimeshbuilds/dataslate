#!/usr/bin/env python3
"""Render an opt-in, pinned Airflow LocalExecutor profile. Never apply or adopt resources."""
import argparse
import copy
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
import yaml

ROOT = Path(__file__).resolve().parent
TRUST = '/etc/airflow-trust/ca.crt'
LABEL = 'dataslate.io/airflow-release'


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,38}[a-z0-9]', value):
        raise ValueError('Expected a 2–40 character Kubernetes name')
    return value


def validate(config):
    keys = {'name', 'namespace', 'image', 'serviceAccount', 'databaseSecret', 'tlsSecret',
            'trustSecret', 'fernetSecret', 'apiSecret', 'jwtSecret', 'gitConnectionSecret',
            'taskConnectionSecret', 'logsClaim', 'parallelism', 'git', 'clientNamespace',
            'clientLabels', 'egress'}
    if not isinstance(config, dict) or set(config) != keys:
        raise ValueError('Missing or unknown managed Airflow profile fields')
    for key in keys - {'image', 'parallelism', 'git', 'clientLabels', 'egress'}:
        name(config[key])
    if (not isinstance(config['image'], str) or not re.fullmatch(
            r'[a-z0-9][a-z0-9./:_-]{1,220}@sha256:[0-9a-f]{64}', config['image'])):
        raise ValueError('An immutable operator-built DataSlate Airflow image is required')
    secrets = [config[key] for key in keys if key.endswith('Secret')]
    if len(set(secrets)) != len(secrets):
        raise ValueError('Each credential and trust Secret must have a distinct name')
    if type(config['parallelism']) is not int or not 1 <= config['parallelism'] <= 8:
        raise ValueError('LocalExecutor parallelism must be 1–8')
    git = config['git']
    if not isinstance(git, dict) or set(git) != {'url', 'commit', 'subdir'}:
        raise ValueError('Expected Git URL, immutable commit and DAG subdirectory')
    try:
        url = urlsplit(git['url'])
        if (not isinstance(git['url'], str) or len(git['url']) > 1024 or url.scheme != 'https'
                or not url.hostname or url.port not in (None, 443) or url.username is not None
                or url.password is not None or url.query or url.fragment
                or re.search(r'[\s{}\\]', git['url'])):
            raise ValueError()
    except Exception:
        raise ValueError('Git requires an HTTPS URL without credentials, query or template syntax') from None
    if not isinstance(git['commit'], str) or not re.fullmatch(r'[a-f0-9]{40}', git['commit']):
        raise ValueError('Git bundle requires a reviewed immutable 40-character commit')
    if (not isinstance(git['subdir'], str) or not re.fullmatch(r'[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*', git['subdir'])
            or len(git['subdir']) > 200):
        raise ValueError('Git DAG directory must be a relative bounded path')
    labels = config['clientLabels']
    if (not isinstance(labels, dict) or not 1 <= len(labels) <= 16
            or any(not isinstance(k, str) or not isinstance(v, str)
                   or not re.fullmatch(r'[A-Za-z0-9_./-]{1,253}', k)
                   or not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', v) for k, v in labels.items())):
        raise ValueError('Explicit client pod labels required')
    if not isinstance(config['egress'], list) or not 1 <= len(config['egress']) <= 64:
        raise ValueError('Explicit bounded database, Git, DataSlate and OAuth egress required')
    for rule in config['egress']:
        if (not isinstance(rule, dict) or set(rule) != {'cidr', 'port'} or type(rule['port']) is not int
                or not 1 <= rule['port'] <= 65535):
            raise ValueError('Invalid egress rule')
        network = ipaddress.ip_network(rule['cidr'], strict=True)
        forbidden = ('0.0.0.0/8', '127.0.0.0/8', '169.254.0.0/16', '224.0.0.0/4') if network.version == 4 else ('::/128', '::1/128', '::ffff:0:0/96', 'fe80::/10', 'ff00::/8')
        if (network.prefixlen == 0 or network.is_multicast or network.is_loopback
                or network.is_link_local or network.is_unspecified
                or any(network.overlaps(ipaddress.ip_network(cidr)) for cidr in forbidden)):
            raise ValueError('Unrestricted, metadata or local egress is not supported')


def secret_env(variable, secret, key):
    return {'name': variable, 'valueFrom': {'secretKeyRef': {'name': secret, 'key': key}}}


def settings(config):
    validate(config)
    repository, digest = config['image'].split('@')
    service = 'https://' + config['name'] + '-api-server.' + config['namespace'] + '.svc:8080'
    security = {'runAsNonRoot': True, 'runAsUser': 50000, 'allowPrivilegeEscalation': False,
                'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']},
                'seccompProfile': {'type': 'RuntimeDefault'}}
    sa = {'create': False, 'name': config['serviceAccount'], 'automountServiceAccountToken': False}
    git_env = secret_env('AIRFLOW_CONN_DATASLATE_GIT', config['gitConnectionSecret'], 'connection')
    resources = {'requests': {'cpu': '250m', 'memory': '512Mi'}, 'limits': {'cpu': '2', 'memory': '2Gi'}}
    common = {'serviceAccount': sa, 'resources': resources,
              'securityContexts': {'container': security},
              'waitForMigrations': {'securityContexts': {'container': security}}}
    api = {**copy.deepcopy(common), 'replicas': 1, 'allowPodLogReading': False,
           'args': ['airflow', 'api-server', '--workers', '1', '--ssl-cert', '/etc/airflow-server-tls/tls.crt',
                    '--ssl-key', '/etc/airflow-server-tls/tls.key'],
           'apiServerConfig': 'from flask_appbuilder.security.manager import AUTH_DB\nAUTH_TYPE = AUTH_DB\nAUTH_USER_REGISTRATION = False\nSESSION_COOKIE_SECURE = True\nSESSION_COOKIE_HTTPONLY = True\nSESSION_COOKIE_SAMESITE = "Lax"\nWTF_CSRF_ENABLED = True\nENABLE_PROXY_FIX = False\n',
           'extraVolumes': [{'name': 'server-tls', 'secret': {'secretName': config['tlsSecret'], 'defaultMode': 288,
                              'items': [{'key': 'tls.crt', 'path': 'tls.crt'}, {'key': 'tls.key', 'path': 'tls.key'}]}}],
           'extraVolumeMounts': [{'name': 'server-tls', 'mountPath': '/etc/airflow-server-tls', 'readOnly': True}],
           'livenessProbe': {'scheme': 'HTTPS'}, 'readinessProbe': {'scheme': 'HTTPS'},
           'startupProbe': {'scheme': 'HTTPS'}, 'service': {'type': 'ClusterIP'}}
    return {
        'fullnameOverride': config['name'], 'useStandardNaming': True, 'airflowVersion': '3.4.0',
        'images': {'airflow': {'repository': repository, 'digest': digest, 'pullPolicy': 'IfNotPresent'},
                   'useDefaultImageForMigration': False},
        'executor': 'LocalExecutor', 'allowPodLaunching': False, 'allowJobLaunching': False,
        'rbac': {'create': False, 'createSCCRoleBinding': False},
        'labels': {LABEL: config['name']}, 'enableBuiltInSecretEnvVars': {'AIRFLOW_CONN_AIRFLOW_DB': False},
        # The upstream chart otherwise creates an unused Celery broker Secret even for LocalExecutor.
        # No container references this disabled broker name; the adaptation asserts that invariant.
        'data': {'metadataSecretName': config['databaseSecret'], 'brokerUrlSecretName': config['name'] + '-unused-broker'},
        'fernetKeySecretName': config['fernetSecret'], 'apiSecretKeySecretName': config['apiSecret'],
        'jwtSecretName': config['jwtSecret'], 'airflowLocalSettings': None,
        'securityContexts': {'pod': {'runAsNonRoot': True, 'runAsUser': 50000, 'fsGroup': 0,
                                    'seccompProfile': {'type': 'RuntimeDefault'}}, 'container': security},
        'env': [{'name': 'PYTHONDONTWRITEBYTECODE', 'value': '1'},
                {'name': 'GIT_TERMINAL_PROMPT', 'value': '0'}, {'name': 'GIT_SSL_CAINFO', 'value': TRUST},
                {'name': 'AWS_EC2_METADATA_DISABLED', 'value': 'true'},
                {'name': 'AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS', 'value': 'false'}],
        'volumes': [{'name': 'trust', 'secret': {'secretName': config['trustSecret'],
                     'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}},
                    {'name': 'profile-temp', 'emptyDir': {'sizeLimit': '256Mi'}},
                    {'name': 'bundle-cache', 'emptyDir': {'sizeLimit': '1Gi'}}],
        'volumeMounts': [{'name': 'trust', 'mountPath': '/etc/airflow-trust', 'readOnly': True},
                        {'name': 'profile-temp', 'mountPath': '/tmp'},
                        {'name': 'bundle-cache', 'mountPath': '/opt/airflow/bundles'}],
        'config': {'core': {'load_examples': 'False', 'parallelism': str(config['parallelism']),
                           'max_active_tasks_per_dag': str(config['parallelism']), 'max_active_runs_per_dag': '1',
                           'dags_are_paused_at_creation': 'True',
                           'auth_manager': 'airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager',
                           'execution_api_server_url': service + '/execution/'},
                   'api': {'base_url': service, 'ssl_ca_file': TRUST, 'client_use_public_certs': 'False'},
                   'api_auth': {'jwt_expiration_time': '900'},
                   'fab': {'enable_proxy_fix': 'False'},
                   'webserver': {'enable_proxy_fix': 'False'},
                   'database': {'load_default_connections': 'False'},
                   'dag_processor': {'dag_bundle_storage_path': '/opt/airflow/bundles'},
                   'logging': {'remote_logging': 'False'}, 'metrics': {'statsd_on': 'False'}},
        'scheduler': {**copy.deepcopy(common), 'replicas': 1, 'strategy': {'type': 'Recreate'},
                      'logGroomerSidecar': {'enabled': False},
                      'args': ['airflow', 'scheduler'],
                      'env': [git_env, secret_env('AIRFLOW_CONN_DATASLATE_DEFAULT', config['taskConnectionSecret'], 'connection')]},
        'dagProcessor': {**copy.deepcopy(common), 'enabled': True, 'replicas': 1,
                         'logGroomerSidecar': {'enabled': False},
                         'args': ['airflow', 'dag-processor'], 'env': [git_env],
                         'dagBundleConfigList': [{'name': 'dataslate-reviewed',
                             'classpath': 'airflow.providers.git.bundles.git.GitDagBundle',
                             'kwargs': {'git_conn_id': 'dataslate_git', 'repo_url': config['git']['url'],
                                        'tracking_ref': config['git']['commit'], 'subdir': config['git']['subdir'],
                                        'submodules': False, 'prune_dotgit_folder': True}}]},
        'apiServer': api,
        'migrateDatabaseJob': {**{k: v for k, v in copy.deepcopy(common).items() if k != 'waitForMigrations'}, 'enabled': True, 'useHelmHooks': False,
                               'args': ['airflow', 'db', 'migrate'], 'restartPolicy': 'Never'},
        'createUserJob': {'enabled': False, 'defaultUser': {'enabled': False}, 'serviceAccount': sa},
        'workers': {'persistence': {'enabled': False}, 'celery': {'persistence': {'enabled': False}}},
        'postgresql': {'enabled': False}, 'redis': {'enabled': False}, 'pgbouncer': {'enabled': False},
        'statsd': {'enabled': False}, 'flower': {'enabled': False}, 'triggerer': {'enabled': False},
        'dags': {'persistence': {'enabled': False}, 'gitSync': {'enabled': False}},
        'logs': {'persistence': {'enabled': True, 'existingClaim': config['logsClaim']}},
        'networkPolicies': {'enabled': False}, 'ingress': {'enabled': False},
    }


def networks(config):
    match = {LABEL: config['name']}
    metadata = {'namespace': config['namespace'], 'labels': match}
    dns = {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                    'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}}}],
           'ports': [{'protocol': 'UDP', 'port': 53}, {'protocol': 'TCP', 'port': 53}]}
    internal = {'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': config['namespace']}},
                'podSelector': {'matchLabels': match}}
    client = {'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': config['clientNamespace']}},
              'podSelector': {'matchLabels': config['clientLabels']}}
    deny = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
            'metadata': {**metadata, 'name': config['name']},
            'spec': {'podSelector': {'matchLabels': match}, 'policyTypes': ['Ingress', 'Egress'], 'ingress': [],
                     'egress': [dns, {'to': [{**internal, 'podSelector': {'matchLabels': {**match, 'component': 'api-server'}}}],
                                     'ports': [{'protocol': 'TCP', 'port': 8080}]}]
                         + [{'to': [{'ipBlock': {'cidr': r['cidr']}}], 'ports': [{'protocol': 'TCP', 'port': r['port']}]}
                            for r in config['egress']]}}
    api = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
           'metadata': {**metadata, 'name': config['name'] + '-api'},
           'spec': {'podSelector': {'matchLabels': {**match, 'component': 'api-server'}}, 'policyTypes': ['Ingress'],
                    'ingress': [{'from': [internal, client], 'ports': [{'protocol': 'TCP', 'port': 8080}]}]}}
    return [deny, api]


def adapt(documents, config):
    documents = copy.deepcopy(documents)
    deployments = set()
    jobs = 0
    result = []
    for document in documents:
        kind = document.get('kind')
        if kind not in ('Deployment', 'Service', 'ConfigMap', 'Job'):
            raise ValueError('Pinned chart emitted an unexpected or externally owned resource')
        meta = document['metadata']
        if meta.get('namespace', config['namespace']) != config['namespace']:
            raise ValueError('Unexpected namespace from pinned chart')
        meta['namespace'] = config['namespace']
        meta.setdefault('annotations', {})['dataslate.io/upstream-profile'] = 'airflow-1.22.0-local-3.4.0'
        if kind == 'Service' and meta['name'] == config['name'] + '-scheduler':
            # Task logs are on the explicit shared PVC. Do not expose the default HTTP log server.
            if document['spec'].get('selector', {}).get('component') != 'scheduler' or [p['port'] for p in document['spec']['ports']] != [8793]:
                raise ValueError('Unexpected scheduler Service shape')
            continue
        result.append(document)
        if kind not in ('Deployment', 'Job'):
            continue
        pod = document['spec']['template']['spec']
        component = document['spec']['template']['metadata']['labels'].get('component')
        if kind == 'Deployment':
            if component not in ('api-server', 'scheduler', 'dag-processor') or component in deployments:
                raise ValueError('Unexpected Airflow component')
            deployments.add(component)
            if document['spec']['replicas'] != 1:
                raise ValueError('This profile requires one replica per component')
            if component == 'scheduler':
                # Do not overlap two LocalExecutor schedulers during a rollout.
                document['spec']['strategy'] = {'type': 'Recreate'}
        else:
            jobs += 1
            if not meta['name'].endswith('-run-airflow-migrations'):
                raise ValueError('Unexpected migration Job')
            document['spec'].update({'backoffLimit': 0, 'activeDeadlineSeconds': 600})
        pod['automountServiceAccountToken'] = False
        pod['enableServiceLinks'] = False
        if pod.get('serviceAccountName') != config['serviceAccount'] or len(pod['containers']) != 1:
            raise ValueError('Unexpected pod identity or sidecar')
        for container in pod.get('initContainers', []) + pod['containers']:
            if container['image'] != config['image'] or not container['securityContext'].get('readOnlyRootFilesystem'):
                raise ValueError('Unexpected image or writable root filesystem')
            # Migration-wait init containers do not need private API server keys or task credentials.
            if container in pod.get('initContainers', []):
                container['volumeMounts'] = [m for m in container.get('volumeMounts', []) if m['name'] != 'server-tls']
            env = {e['name']: e for e in container.get('env', [])}
            if 'AIRFLOW_CONN_DATASLATE_DEFAULT' in env and (component != 'scheduler' or container in pod.get('initContainers', [])):
                raise ValueError('Task credential crossed its scheduler boundary')
            if 'AIRFLOW_CONN_DATASLATE_GIT' in env and component not in ('scheduler', 'dag-processor'):
                raise ValueError('Git credential crossed its code-loading boundary')
            for variable in ('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'AIRFLOW__CORE__FERNET_KEY'):
                if 'secretKeyRef' not in env.get(variable, {}).get('valueFrom', {}):
                    raise ValueError('Required external Secret reference is missing')
    if deployments != {'api-server', 'scheduler', 'dag-processor'} or jobs != 1:
        raise ValueError('Unexpected pinned chart workload shape')
    if config['name'] + '-unused-broker' in json.dumps(result):
        raise ValueError('Disabled broker unexpectedly referenced by chart')
    return result + networks(config)


def run_helm(command, *, timeout=45):
    """Bound output/time and never expose Helm stderr or unsanitized exceptions."""
    with tempfile.TemporaryDirectory(prefix='dataslate-airflow-helm-home-') as home:
        environment = {**os.environ, 'HELM_PLUGINS': home, 'HELM_CACHE_HOME': home,
                       'HELM_CONFIG_HOME': home, 'HELM_DATA_HOME': home}
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True, env=environment)
        except OSError:
            raise ValueError('Helm executable could not be started') from None
        output = bytearray(); total = 0; deadline = time.monotonic() + timeout
        selector = selectors.DefaultSelector()
        for stream in (process.stdout, process.stderr):
            selector.register(stream, selectors.EVENT_READ)
        try:
            while selector.get_map():
                if time.monotonic() > deadline:
                    raise ValueError('Helm rendering exceeded its time limit')
                for key, _ in selector.select(0.2):
                    part = os.read(key.fileobj.fileno(), 65536)
                    if not part:
                        selector.unregister(key.fileobj); continue
                    total += len(part)
                    if total > 8 * 1024 * 1024:
                        raise ValueError('Helm rendering exceeded its output limit')
                    if key.fileobj is process.stdout:
                        output.extend(part)
            if process.wait(timeout=max(0.1, deadline - time.monotonic())) != 0:
                raise ValueError('Pinned Helm chart rendering failed; no manifests produced')
            return output.decode('utf-8')
        finally:
            selector.close()
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdout.close(); process.stderr.close()


def render(config, helm, chart):
    values = settings(config)
    lock = json.loads((ROOT / 'upstream-lock.json').read_text())
    with Path(chart).open('rb') as source:
        data = source.read(8 * 1024 * 1024 + 1)
    if len(data) > 8 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != lock['sha256']:
        raise ValueError('Upstream Airflow chart checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='dataslate-airflow-render-') as directory:
        path = Path(directory) / 'values.json'
        path.write_text(json.dumps(values))
        pinned = Path(directory) / 'airflow.tgz'; pinned.write_bytes(data)
        output = run_helm([helm, 'template', config['name'], str(pinned), '--namespace', config['namespace'],
                           '--skip-tests', '--kube-version', '1.30.13', '-f', str(path)])
    return adapt([doc for doc in yaml.safe_load_all(output) if doc], config)


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate profile field')
        result[key] = value
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--helm', required=True)
    parser.add_argument('--chart', required=True)
    args = parser.parse_args()
    try:
        raw = Path(args.config).read_bytes()
        if len(raw) > 32768:
            raise ValueError()
        documents = render(json.loads(raw, object_pairs_hook=unique_fields), args.helm, args.chart)
    except Exception:
        raise SystemExit('Managed Airflow profile rejected or could not render; validate the documented profile and pinned prerequisites') from None
    print(yaml.safe_dump_all(documents, sort_keys=False))

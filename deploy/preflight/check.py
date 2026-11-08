#!/usr/bin/env python3
"""Read-only, context-pinned installation inventory. Never a readiness certificate."""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time

import yaml


ROOT = Path(__file__).resolve().parents[2]
MAX_BYTES = 4 * 1024 * 1024
MAX_OBJECTS = 500
NAME = re.compile(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?\Z")
LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
KEY = re.compile(r"[-a-zA-Z0-9_.]{1,253}\Z")
META_TEMPLATE = (r'{{.metadata.uid}}{{"\n"}}'
                 r'{{index .metadata.labels "app.kubernetes.io/managed-by"}}{{"\n"}}'
                 r'{{index .metadata.annotations "meta.helm.sh/release-name"}}{{"\n"}}'
                 r'{{index .metadata.annotations "meta.helm.sh/release-namespace"}}{{"\n"}}')
SECRET_TEMPLATE = r'{{"KEYS"}}{{"\n"}}{{range $key, $unused := .data}}{{$key}}{{"\n"}}{{end}}'
DEPLOYMENT_TEMPLATE = (META_TEMPLATE + r'{{.metadata.generation}}{{"\n"}}'
                       r'{{.status.observedGeneration}}{{"\n"}}'
                       r'{{.spec.replicas}}{{"\n"}}{{.status.availableReplicas}}{{"\n"}}'
                       r'{{range .spec.template.spec.containers}}{{.image}}{{"\n"}}{{end}}')
RESOURCE_NAMES = {
    'Deployment': 'deployments.apps', 'Service': 'services',
    'ServiceAccount': 'serviceaccounts', 'ConfigMap': 'configmaps',
    'Role': 'roles.rbac.authorization.k8s.io',
    'RoleBinding': 'rolebindings.rbac.authorization.k8s.io',
    'NetworkPolicy': 'networkpolicies.networking.k8s.io',
    'PodDisruptionBudget': 'poddisruptionbudgets.policy',
    'Ingress': 'ingresses.networking.k8s.io', 'Job': 'jobs.batch',
    'Cluster': 'clusters.postgresql.cnpg.io',
}
CRDS = {
    'spark': ('sparkapplications.spark.apache.org', 'spark.apache.org', 'SparkApplication', [
        ('spec.runtimeVersions.sparkVersion', 'string'), ('spec.sparkConf', 'object'),
        ('spec.driverSpec.podTemplateSpec', 'object'), ('spec.executorSpec.podTemplateSpec', 'object'),
        ('spec.applicationTolerations.restartConfig.restartPolicy', 'string'),
        ('spec.applicationTolerations.resourceRetainPolicy', 'string'),
    ]),
    'postgres': ('clusters.postgresql.cnpg.io', 'postgresql.cnpg.io', 'Cluster', [
        ('spec.instances', 'integer'), ('spec.imageName', 'string'),
        ('spec.storage.storageClass', 'string'), ('spec.storage.size', 'string'),
        ('spec.bootstrap.initdb.database', 'string'), ('spec.bootstrap.initdb.owner', 'string'),
    ]),
}


class InvalidInput(Exception):
    """Deliberately contains no input text or subprocess output."""


class StrictLoader(yaml.SafeLoader):
    pass


def mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str) or key in result:
            raise InvalidInput('YAML keys must be unique strings; merge keys are not supported')
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)


def bounded_tree(value, depth=0, ancestors=None, budget=None):
    ancestors = set() if ancestors is None else ancestors
    budget = [100000] if budget is None else budget
    budget[0] -= 1
    if depth > 40 or budget[0] < 0:
        raise InvalidInput('YAML structure exceeds the supported bound')
    if isinstance(value, (dict, list)):
        if id(value) in ancestors:
            raise InvalidInput('Recursive YAML aliases are not supported')
        next_ancestors = ancestors | {id(value)}
        for child in (value.values() if isinstance(value, dict) else value):
            bounded_tree(child, depth + 1, next_ancestors, budget)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise InvalidInput('YAML values must be JSON-compatible')


def documents(raw):
    if len(raw) > MAX_BYTES:
        raise InvalidInput('Input exceeds the supported byte bound')
    try:
        result = list(yaml.load_all(raw, Loader=StrictLoader))
        bounded_tree(result)
    except (yaml.YAMLError, RecursionError, UnicodeError, ValueError) as exc:
        raise InvalidInput('YAML could not be parsed safely') from exc
    if len(result) > MAX_OBJECTS:
        raise InvalidInput('Too many YAML documents')
    return result


def merge(base, overlay):
    """Helm-style recursive maps and replacement lists; null deletes a map key."""
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def read_values(paths, fingerprints=None):
    result = {}
    for path in paths:
        try:
            with Path(path).open('rb') as stream:
                raw = stream.read(MAX_BYTES + 1)
        except OSError as exc:
            raise InvalidInput('A values file could not be read') from exc
        parsed = documents(raw)
        if len(parsed) != 1 or not isinstance(parsed[0], dict):
            raise InvalidInput('Each values file must contain exactly one mapping')
        result = merge(result, parsed[0])
        if fingerprints is not None:
            fingerprints.append(hashlib.sha256(raw).digest())
    return result


def execute(command, timeout=15):
    """Bound both streams before buffering; never return stderr text to reports."""
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
    except OSError:
        return {'state': 'unavailable'}
    streams = selectors.DefaultSelector()
    output, errors = bytearray(), bytearray()
    streams.register(process.stdout, selectors.EVENT_READ, output)
    streams.register(process.stderr, selectors.EVENT_READ, errors)
    deadline = time.monotonic() + timeout
    failure = None
    try:
        while streams.get_map():
            if time.monotonic() >= deadline:
                failure = 'timeout'
                break
            for key, _ in streams.select(min(0.1, max(0, deadline - time.monotonic()))):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    streams.unregister(key.fileobj)
                else:
                    key.data.extend(chunk)
                    if len(output) + len(errors) > MAX_BYTES:
                        failure = 'output-limit'
                        break
            if failure:
                break
        if failure:
            # Also stop an exec credential plugin that inherited the subprocess pipes.
            import signal
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        failure = 'timeout'
    finally:
        streams.close()
        process.stdout.close()
        process.stderr.close()
    if failure:
        return {'state': failure}
    if process.returncode:
        error = errors.decode('utf-8', errors='replace').lower()
        category = 'permission-denied' if ('(forbidden)' in error or '(unauthorized)' in error) else 'command-failed'
        return {'state': category}
    try:
        return {'state': 'ok', 'text': output.decode('utf-8')}
    except UnicodeError:
        return {'state': 'invalid-output'}


def valid_name(value, label=False):
    if not isinstance(value, str) or not (LABEL if label else NAME).fullmatch(value):
        raise InvalidInput('A resource name or namespace is invalid')
    return value


def present(value):
    return value not in ('', '<no value>')


def safe_owner(parts):
    if len(parts) < 4 or not present(parts[0]) or any(len(part) > 253 or not re.fullmatch(r'[A-Za-z0-9_.-]*|<no value>', part) for part in parts[:4]):
        return None
    return {'uid': parts[0], 'managedBy': parts[1] if present(parts[1]) else None,
            'release': parts[2] if present(parts[2]) else None,
            'namespace': parts[3] if present(parts[3]) else None}


def schema_field(schema, path):
    for part in path.split('.'):
        schema = schema.get('properties', {}).get(part, {})
    return schema


def secret_references(manifests, namespace):
    refs = {}

    def add(ns, name, keys=(), optional=False):
        valid_name(ns, label=True)
        valid_name(name)
        if not all(isinstance(key, str) and KEY.fullmatch(key) for key in keys):
            raise InvalidInput('A Secret key reference is invalid')
        item = refs.setdefault((ns, name), {'keys': set(), 'required': False})
        item['keys'].update(keys)
        item['required'] |= not optional

    def walk(obj, ns):
        if isinstance(obj, list):
            for child in obj:
                walk(child, ns)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                if key == 'secretKeyRef' and isinstance(value, dict):
                    add(ns, value.get('name'), [value.get('key')], value.get('optional', False))
                elif key == 'secretRef' and isinstance(value, dict):
                    add(ns, value.get('name'), optional=value.get('optional', False))
                elif key == 'secret' and isinstance(value, dict) and ('secretName' in value or 'name' in value):
                    add(ns, value.get('secretName', value.get('name')),
                        [item.get('key') for item in value.get('items', [])], value.get('optional', False))
                elif key == 'imagePullSecrets' and isinstance(value, list):
                    for item in value:
                        add(ns, item.get('name'))
                walk(value, ns)

    for obj in manifests:
        ns = obj.get('metadata', {}).get('namespace', namespace)
        walk(obj, ns)
        if obj.get('kind') == 'Ingress':
            for tls in obj.get('spec', {}).get('tls', []):
                if tls.get('secretName'):
                    add(ns, tls['secretName'], ['tls.crt', 'tls.key'])
    return refs


class Preflight:
    def __init__(self, args, runner=execute):
        self.args, self.runner = args, runner
        self.checks = []
        self.started = time.monotonic()
        self.calls = 0

    def add(self, check_id, status, message, action=None, evidence=None):
        item = {'id': check_id, 'status': status, 'message': message}
        if action:
            item['action'] = action
        if evidence is not None:
            item['evidence'] = evidence
        self.checks.append(item)

    def invoke(self, command):
        remaining = self.args.total_timeout - (time.monotonic() - self.started)
        if remaining <= 0 or self.calls >= 300:
            return {'state': 'budget-exhausted'}
        self.calls += 1
        return self.runner(command, min(self.args.timeout, remaining))

    def kubectl(self, arguments, parse_json=False):
        command = [self.args.kubectl, '--context', self.args.context,
                   '--request-timeout', f'{self.args.timeout}s', *arguments]
        result = self.invoke(command)
        if result['state'] == 'ok' and parse_json:
            try:
                result['object'] = json.loads(result.pop('text'))
                bounded_tree(result['object'])
                if not isinstance(result['object'], dict):
                    raise ValueError()
            except (ValueError, TypeError, InvalidInput, RecursionError):
                result = {'state': 'invalid-output'}
        return result

    def get(self, resource, name=None, namespace=None, template=None):
        command = ['get', resource]
        if name:
            command += [name, '--ignore-not-found=true']
        if namespace:
            command += ['--namespace', namespace]
        command += ['-o', 'go-template=' + template] if template else ['-o', 'json']
        # Missing named resources produce no JSON, separately from API/RBAC errors.
        result = self.kubectl(command)
        if result['state'] == 'ok' and not result['text'].strip():
            return {'state': 'absent'}
        if result['state'] == 'ok' and not template:
            try:
                result['object'] = json.loads(result.pop('text'))
                bounded_tree(result['object'])
                if not isinstance(result['object'], dict):
                    raise ValueError()
            except (ValueError, TypeError, InvalidInput, RecursionError):
                return {'state': 'invalid-output'}
        return result

    def unavailable(self, check_id, result, action):
        self.add(check_id, 'unknown', 'The read-only probe did not establish this prerequisite.', action,
                 {'probeState': result['state']})

    def version(self):
        result = self.kubectl(['version', '-o', 'json'], parse_json=True)
        if result['state'] != 'ok':
            self.unavailable('cluster.version', result, 'Verify the explicit context, API reachability and kubectl access.')
            return
        version = result['object'].get('serverVersion', {}).get('gitVersion', '')
        match = re.fullmatch(r'v(\d+)\.(\d+)\.\d+(?:[-+][A-Za-z0-9_.-]+)?', version)
        if not match:
            self.unavailable('cluster.version', {'state': 'invalid-output'}, 'Read a recognized Kubernetes server version.')
            return
        supported = (int(match[1]), int(match[2])) >= (1, 29)
        self.add('cluster.version', 'observed' if supported else 'blocked',
                 'Server version meets the chart minimum; integrated compatibility remains unqualified.' if supported else 'Server version is below the chart minimum of Kubernetes 1.29.',
                 evidence={'serverVersion': version, 'chartMinimum': '1.29.0'})

    def namespace(self, namespace, compute=False):
        result = self.get('namespaces', namespace)
        check_id = f'namespace.{namespace}'
        if result['state'] == 'absent':
            self.add(check_id, 'blocked', 'Required namespace does not exist.',
                     'Create the namespace through your reviewed ownership and isolation workflow.')
        elif result['state'] != 'ok':
            self.unavailable(check_id, result, 'Grant read access to the exact namespace or have its owner supply the inventory.')
        else:
            obj = result['object']
            if obj.get('metadata', {}).get('deletionTimestamp') or obj.get('status', {}).get('phase') != 'Active':
                self.add(check_id, 'blocked', 'Required namespace is not Active.')
            else:
                self.add(check_id, 'observed', 'Required namespace is Active.')
            if compute:
                labels = obj.get('metadata', {}).get('labels', {})
                restricted = labels.get('pod-security.kubernetes.io/enforce') == 'restricted'
                version = labels.get('pod-security.kubernetes.io/enforce-version', '')
                pinned = bool(re.fullmatch(r'v1\.\d+', version))
                self.add(check_id + '.pod-security', 'observed' if restricted and pinned else 'blocked',
                         'Restricted Pod Security is explicitly version-pinned.' if restricted and pinned else 'Compute namespace lacks version-pinned restricted Pod Security.',
                         'Review deploy/compute/ and qualify admission with the intended workload identity.')
        if compute:
            for resource in ('resourcequotas', 'limitranges', 'networkpolicies.networking.k8s.io'):
                result = self.get(resource, namespace=namespace)
                if result['state'] != 'ok':
                    self.unavailable(check_id + '.' + resource, result, 'Have the compute namespace owner verify quotas, limits and network isolation.')
                    continue
                items = result['object'].get('items')
                if not isinstance(items, list):
                    self.unavailable(check_id + '.' + resource, {'state': 'invalid-output'}, 'Repeat the namespace inventory.')
                    continue
                self.add(check_id + '.' + resource, 'observed' if items else 'blocked',
                         'Resource presence observed; policy contents and enforcement are not qualified.' if items else 'Required compute boundary resource is absent.',
                         'Review effective policy, capacity and admission; resource counts alone do not establish isolation.',
                         {'count': len(items)})

    def crd(self, kind):
        name, group, resource_kind, fields = CRDS[kind]
        result = self.get('customresourcedefinitions.apiextensions.k8s.io', name)
        check_id = 'operator.' + kind + '.crd'
        if result['state'] == 'absent':
            self.add(check_id, 'blocked', 'Required operator CRD is absent.',
                     'Review deploy/bootstrap/ and separately install the pinned operator only after checking existing ownership.')
            return
        if result['state'] != 'ok':
            self.unavailable(check_id, result, 'Ask the cluster operator owner for read access to this exact CRD.')
            return
        obj = result['object']
        spec = obj.get('spec', {})
        versions = spec.get('versions', [])
        selected = [v for v in versions if v.get('name') == 'v1' and v.get('served') is True]
        established = any(c.get('type') == 'Established' and c.get('status') == 'True' for c in obj.get('status', {}).get('conditions', []))
        valid = (spec.get('group') == group and spec.get('scope') == 'Namespaced'
                 and spec.get('names', {}).get('kind') == resource_kind
                 and not obj.get('metadata', {}).get('deletionTimestamp') and established and len(selected) == 1)
        missing = []
        if valid:
            schema = selected[0].get('schema', {}).get('openAPIV3Schema', {})
            missing = [path for path, expected in fields if schema_field(schema, path).get('type') != expected]
        served = sorted(v.get('name') for v in versions if v.get('served') is True and isinstance(v.get('name'), str) and re.fullmatch(r'v\d+(?:(?:alpha|beta)\d+)?', v['name']))
        self.add(check_id, 'observed' if valid and not missing else 'blocked',
                 'Established namespaced v1 CRD has the required field types.' if valid and not missing else 'CRD identity, establishment, served v1 or required field types do not match.',
                 'Compare the complete schema and operator/runtime versions with the pinned profile; these field checks do not certify compatibility.',
                 {'name': name, 'servedVersions': served, 'missingOrIncompatibleFields': missing})

    def operator(self, kind, location):
        check_id = 'operator.' + kind + '.controller'
        if not location:
            self.add(check_id, 'unknown', 'The responsible operator Deployment has not been identified.',
                     f'Provide --{kind}-operator NAMESPACE/NAME from the existing operator owner; CRD presence does not identify its controller.')
            return
        namespace, name = location.split('/')
        result = self.get('deployments.apps', name, namespace, DEPLOYMENT_TEMPLATE)
        if result['state'] == 'absent':
            self.add(check_id, 'blocked', 'The explicitly identified operator Deployment is absent.')
            return
        if result['state'] != 'ok':
            self.unavailable(check_id, result, 'Verify this exact Deployment with its current owner.')
            return
        parts = result['text'].splitlines()
        owner = safe_owner(parts)
        numbers = parts[4:8]
        images = parts[8:]
        if not owner or len(numbers) != 4 or not all(re.fullmatch(r'\d+|<no value>', x) for x in numbers) or not images or not all(re.fullmatch(r'[a-zA-Z0-9_./:@-]{1,512}', x) for x in images):
            self.unavailable(check_id, {'state': 'invalid-output'}, 'Inspect the named operator Deployment without exposing environment variables.')
            return
        generation, observed, wanted, available = [0 if x == '<no value>' else int(x) for x in numbers]
        healthy = wanted > 0 and available >= wanted and observed >= generation
        self.add(check_id, 'observed' if healthy else 'blocked',
                 'Deployment reports its requested replicas available; ownership remains unchanged.' if healthy else 'Operator Deployment does not report current-generation availability.',
                 'Confirm image versions, namespace watch scope, admission webhooks and compatibility with the existing owner; no adoption is performed.',
                 {'namespace': namespace, 'name': name, 'owner': owner, 'images': images, 'availableReplicas': available})

    def ownership(self, manifests):
        for obj in manifests:
            kind, meta = obj.get('kind'), obj.get('metadata', {})
            if not isinstance(kind, str) or not re.fullmatch(r'[A-Z][A-Za-z0-9]{0,63}', kind):
                raise InvalidInput('Rendered resource kind is invalid')
            name, namespace = valid_name(meta.get('name')), valid_name(meta.get('namespace', self.args.namespace), label=True)
            check_id = f'ownership.{namespace}.{kind}.{name}'
            resource = RESOURCE_NAMES.get(kind)
            if not resource:
                self.add(check_id, 'unknown', 'Rendered resource kind is outside the supported ownership inventory.',
                         'Review this resource and its owner explicitly; preflight will not adopt it.')
                continue
            result = self.get(resource, name, namespace, META_TEMPLATE)
            if result['state'] == 'absent':
                self.add(check_id, 'observed', 'No object currently occupies this rendered name.')
            elif result['state'] != 'ok':
                self.unavailable(check_id, result, 'Obtain read access to this exact resource and verify its current owner.')
            else:
                owner = safe_owner(result['text'].splitlines())
                if not owner:
                    self.unavailable(check_id, {'state': 'invalid-output'}, 'Inspect existing ownership metadata through the resource owner.')
                    continue
                ours = owner['managedBy'] == 'Helm' and owner['release'] == self.args.release and owner['namespace'] == self.args.namespace
                self.add(check_id, 'observed' if ours else 'blocked',
                         'Existing object declares this Helm release; this is not an upgrade safety check.' if ours else 'Existing object is unowned or owned by a different release.',
                         'Preserve the current owner. Select a free name or arrange an explicit migration; never patch ownership labels to bypass this check.', owner)

    def secrets(self, manifests):
        generated = set()
        for obj in manifests:
            if obj.get('kind') == 'Cluster' and obj.get('apiVersion') == 'postgresql.cnpg.io/v1':
                generated.update({obj['metadata']['name'] + '-app', obj['metadata']['name'] + '-ca'})
        for (namespace, name), ref in sorted(secret_references(manifests, self.args.namespace).items()):
            result = self.get('secrets', name, namespace, SECRET_TEMPLATE)
            check_id = f'secret.{namespace}.{name}'
            evidence = {'namespace': namespace, 'name': name, 'requiredKeys': sorted(ref['keys'])}
            if result['state'] == 'absent':
                pending = name in generated and namespace == self.args.namespace
                self.add(check_id, 'pending' if pending or not ref['required'] else 'blocked',
                         'Secret will be generated by the separately verified CNPG controller.' if pending else 'Referenced Secret is absent.',
                         'Re-run preflight after the database operator has reconciled.' if pending else 'Have the credential owner provision the named Secret and keys without placing values in reports.', evidence)
            elif result['state'] != 'ok':
                self.unavailable(check_id, result, 'Grant get on this named Secret or have its owner verify key names; do not grant list on Secrets.')
            else:
                lines = result['text'].splitlines()
                keys = lines[1:]
                if not lines or lines[0] != 'KEYS' or len(keys) > 4096 or any(not KEY.fullmatch(key) for key in keys) or len(set(keys)) != len(keys):
                    self.unavailable(check_id, {'state': 'invalid-output'}, 'Have the credential owner verify the named Secret keys.')
                    continue
                missing = sorted(ref['keys'] - set(keys))
                evidence['missingKeys'] = missing
                self.add(check_id, 'blocked' if missing else 'observed',
                         'Required Secret keys are missing.' if missing else 'Required Secret key names exist; values, validity and rotation were not inspected.',
                         'Verify credential validity, CA trust, least privilege and rotation separately.', evidence)

    def classes(self, values):
        postgres = values.get('postgres', {})
        if postgres.get('mode') == 'managed':
            name = postgres.get('managed', {}).get('storageClass')
            self.named_class('storageclasses.storage.k8s.io', name, 'storage.class',
                             'Verify CSI controller readiness, capacity, topology, encryption, reclaim policy and backup/restore separately.')
        ingress = values.get('ingress', {})
        if ingress.get('enabled'):
            self.named_class('ingressclasses.networking.k8s.io', ingress.get('className'), 'ingress.class',
                             'Verify the owning ingress controller, certificate, host routing and external load balancer separately.')

    def named_class(self, resource, name, check_id, action):
        if not name:
            self.add(check_id, 'unknown', 'No explicit class was selected; defaults are not assumed.', action)
            return
        result = self.get(resource, valid_name(name))
        if result['state'] == 'absent':
            self.add(check_id, 'blocked', 'The configured class does not exist.', action, {'name': name})
        elif result['state'] != 'ok':
            self.unavailable(check_id, result, 'Ask the cluster owner to verify this exact class.')
        else:
            self.add(check_id, 'observed', 'The configured class exists; its controller and end-to-end behavior are unqualified.', action, {'name': name})

    def run(self):
        paths = [self.args.chart / 'values.yaml', *self.args.values]
        fingerprints = []
        values = read_values(paths, fingerprints)
        render = [self.args.helm, 'template', self.args.release, str(self.args.chart),
                  '--namespace', self.args.namespace, '--kube-version', '1.29.0',
                  '--api-versions', 'postgresql.cnpg.io/v1']
        for path in self.args.values:
            render += ['--values', str(path)]
        result = self.invoke(render)
        after = []
        read_values(paths, after)
        if after != fingerprints:
            raise InvalidInput('Values changed while the chart was rendering; repeat with stable files')
        if result['state'] != 'ok':
            self.add('chart.render', 'blocked', 'The local chart did not render successfully.',
                     'Run helm lint/template locally with these values to resolve validation or tooling errors; raw output is intentionally omitted.', {'probeState': result['state']})
            return self.report()
        manifests = [obj for obj in documents(result['text'].encode()) if obj is not None]
        if not manifests or not all(isinstance(obj, dict) for obj in manifests):
            raise InvalidInput('Rendered chart must contain Kubernetes object mappings')
        self.add('chart.render', 'observed', 'Local chart rendered; this is not server admission, installation or health verification.',
                 evidence={'objects': len(manifests)})
        self.version()
        namespaces = set(values.get('execution', {}).get('namespaces', [])) if values.get('execution', {}).get('enabled') else set()
        namespaces.add(self.args.namespace)
        for namespace in sorted(namespaces):
            self.namespace(valid_name(namespace, label=True), compute=namespace in values.get('execution', {}).get('namespaces', []) and values.get('execution', {}).get('enabled', False))
        execution = values.get('execution', {}).get('enabled', False)
        managed = values.get('postgres', {}).get('mode') == 'managed'
        if execution:
            self.crd('spark')
            self.operator('spark', self.args.spark_operator)
            other = self.get('customresourcedefinitions.apiextensions.k8s.io', 'sparkapplications.sparkoperator.k8s.io')
            if other['state'] == 'ok':
                self.add('operator.kubeflow', 'unsupported', 'Kubeflow Spark CRD is also installed; DataSlate execution uses Apache spark.apache.org/v1 only.',
                         'Preserve the existing Kubeflow owner and review coexistence; preview support does not imply execution compatibility.')
            elif other['state'] not in ('absent', 'ok'):
                self.unavailable('operator.kubeflow', other, 'Confirm whether a separate Kubeflow operator already exists before any bootstrap.')
            else:
                self.add('operator.kubeflow', 'observed', 'No Kubeflow Spark CRD was found.')
            self.add('compute.enforcement', 'unknown', 'Workload identity, driver privileges, admission behavior and storage permissions require live qualification.',
                     'Use deploy/compute/ admission checks in an authorized isolated cluster and verify Spark driver/executor IAM and S3/KMS permissions.')
        if managed:
            self.crd('postgres')
            self.operator('postgres', self.args.postgres_operator)
        self.ownership(manifests)
        self.secrets(manifests)
        self.classes(values)
        if values.get('networkPolicy', {}).get('enabled'):
            result = self.kubectl(['get', '--raw', '/apis/networking.k8s.io/v1'], parse_json=True)
            resources = result.get('object', {}).get('resources', [])
            exists = any(r.get('name') == 'networkpolicies' and r.get('namespaced') is True for r in resources)
            self.add('network.api', 'observed' if exists else 'unknown',
                     'NetworkPolicy API is served.' if exists else 'NetworkPolicy API availability was not established.',
                     'Verify CNI enforcement, additive policies, DNS, ingress and destination/SNAT behavior on the target cluster.')
        else:
            self.add('network.policy', 'unknown', 'Chart NetworkPolicies are disabled; external isolation must be independently verified.',
                     'Identify the owner of effective network isolation and record live enforcement tests.')
        self.add('network.enforcement', 'unknown', 'Read-only API discovery cannot establish effective network isolation or endpoint reachability.',
                 'Qualify CNI enforcement and actual TLS/authentication paths; this tool never opens data-plane connections.')
        modules = [key for key in ('execution', 'sql', 'catalog', 'search', 'sylon', 'publication', 'workflow') if values.get(key, {}).get('enabled')]
        self.add('integrations.live', 'unknown', 'Configured integrations require independent end-to-end qualification.',
                 'Verify OIDC, database migrations/permissions/restore and each enabled backend with the supported acceptance journey.',
                 {'databaseMode': 'managed' if managed else 'external', 'enabledModules': modules,
                  'backendOwnership': 'external; optional service profiles are separate reviewed deployments'})
        return self.report()

    def report(self):
        statuses = [check['status'] for check in self.checks]
        state = 'blocked' if 'blocked' in statuses else ('incomplete' if any(s in statuses for s in ('unknown', 'pending', 'unsupported')) else 'prerequisites-observed')
        return {'schemaVersion': 1, 'product': 'DataSlate', 'readOnly': True,
                'qualification': 'not-performed', 'context': self.args.context,
                'namespace': self.args.namespace, 'release': self.args.release,
                'generatedAt': dt.datetime.now(dt.timezone.utc).isoformat(),
                'state': state, 'counts': {s: statuses.count(s) for s in sorted(set(statuses))},
                'checks': self.checks}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--context', required=True, help='Explicit kubeconfig context; current-context is never inferred')
    result.add_argument('--namespace', required=True)
    result.add_argument('--release', default='dataslate')
    result.add_argument('-f', '--values', action='append', default=[], type=Path)
    result.add_argument('--chart', type=Path, default=ROOT / 'deploy/helm/dataslate')
    result.add_argument('--spark-operator', metavar='NAMESPACE/NAME')
    result.add_argument('--postgres-operator', metavar='NAMESPACE/NAME')
    result.add_argument('--kubectl', default='kubectl', help='Trusted kubectl executable')
    result.add_argument('--helm', default='helm', help='Trusted Helm executable')
    result.add_argument('--timeout', default=15, type=int, help='Per-command timeout in seconds, 1–60')
    result.add_argument('--total-timeout', default=180, type=int, help='Total probe budget in seconds, 1–600')
    result.add_argument('--format', choices=('text', 'json'), default='text')
    return result


def validate_arguments(args):
    valid_name(args.namespace, label=True)
    valid_name(args.release, label=True)
    if len(args.release) > 53:
        raise InvalidInput('Helm release name exceeds 53 characters')
    if not args.context or len(args.context) > 512 or not args.context.isprintable() or args.context.startswith('-'):
        raise InvalidInput('Context must be an explicit non-option name without control characters')
    if not 1 <= args.timeout <= 60 or not 1 <= args.total_timeout <= 600 or len(args.values) > 12:
        raise InvalidInput('Timeout or values-file count exceeds supported bounds')
    for location in (args.spark_operator, args.postgres_operator):
        if location:
            if len(location.split('/')) != 2:
                raise InvalidInput('Operator location must be NAMESPACE/NAME')
            namespace, name = location.split('/')
            valid_name(namespace, label=True)
            valid_name(name)
    args.chart = args.chart.resolve()
    args.values = [path.resolve() for path in args.values]


def render_text(report):
    lines = [f'DataSlate installation preflight: {report["state"]}',
             f'Context: {report["context"]} | namespace: {report["namespace"]} | release: {report["release"]}',
             'Read-only inventory. No installation or live qualification performed.', '']
    for check in report['checks']:
        lines.append(f'[{check["status"].upper()}] {check["id"]}: {check["message"]}')
        if check.get('action'):
            lines.append('  Next: ' + check['action'])
        if check.get('evidence'):
            lines.append('  Evidence: ' + json.dumps(check['evidence'], sort_keys=True))
    return '\n'.join(lines)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        validate_arguments(args)
        report = Preflight(args).run()
    except (InvalidInput, KeyError, TypeError, AttributeError, ValueError):
        # Do not echo a YAML parser context or arbitrary values/cluster response.
        print('Preflight input or response is invalid; verify the chart, values and resource shapes. Raw content was not printed.', file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True) if args.format == 'json' else render_text(report))
    return {'prerequisites-observed': 0, 'blocked': 1, 'incomplete': 3}[report['state']]


if __name__ == '__main__':
    raise SystemExit(main())

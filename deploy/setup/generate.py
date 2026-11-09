#!/usr/bin/env python3
"""Generate a review bundle for a new core installation; never contact Kubernetes."""
from __future__ import annotations
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / 'deploy/helm/dataslate'
MAX_INPUT = 65536
MAX_RENDER = 4 * 1024 * 1024


class Invalid(ValueError):
    pass


def obj(value, required, optional=(), label='profile'):
    if not isinstance(value, dict) or set(value) - set(required) - set(optional) or set(required) - set(value):
        raise Invalid(f'{label}: missing or unknown fields')
    return value


def string(value, label, pattern=None, maximum=253):
    if not isinstance(value, str) or not 1 <= len(value) <= maximum or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise Invalid(f'{label}: invalid string')
    if pattern and not re.fullmatch(pattern, value):
        raise Invalid(f'{label}: invalid format')
    return value


def name(value, label, maximum=63):
    return string(value, label, r'[a-z0-9](?:[-a-z0-9]*[a-z0-9])?', maximum)


def secret(value, label):
    obj(value, ['name', 'key'], label=label)
    name(value['name'], label + '.name')
    string(value['key'], label + '.key', r'[A-Za-z0-9._-]+')
    return dict(value)


def labels(value, label):
    if not isinstance(value, dict) or not 1 <= len(value) <= 12:
        raise Invalid(f'{label}: require 1–12 exact pod labels')
    for key, val in value.items():
        string(key, label, r'(?:[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?/)?[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?')
        string(val, label, r'[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?', 63)
    return dict(value)


def endpoint(value, label, origin=False):
    string(value, label, maximum=2048)
    if any(c.isspace() for c in value) or '\\' in value:
        raise Invalid(f'{label}: invalid HTTPS endpoint')
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise Invalid(f'{label}: invalid HTTPS endpoint') from None
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise Invalid(f'{label}: require HTTPS without credentials, query or fragment')
    if port == 0 or parsed.hostname == 'localhost' or parsed.hostname.endswith('.localhost'):
        raise Invalid(f'{label}: require a non-loopback production endpoint and valid port')
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        address = None
    if address and (address.is_loopback or address.is_link_local or address.is_unspecified or address.is_multicast):
        raise Invalid(f'{label}: local or metadata endpoint is forbidden')
    string(parsed.hostname, label, r'[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?', 253)
    if origin and (parsed.path not in ('', '/') or port not in (None, 443)):
        raise Invalid(f'{label}: require an HTTPS origin on port 443')
    if '%' in parsed.path or '..' in parsed.path.split('/') or any(c in value for c in ['"', "'", '`', '$', '{', '}']):
        raise Invalid(f'{label}: unsupported URL characters')
    return parsed


def cidrs(value, label):
    if not isinstance(value, list) or not 1 <= len(value) <= 24:
        raise Invalid(f'{label}: require 1–24 explicit destination CIDRs')
    blocked = [ipaddress.ip_network(x) for x in ['0.0.0.0/8', '127.0.0.0/8', '169.254.0.0/16', '224.0.0.0/4', '240.0.0.0/4', '::/128', '::1/128', 'fe80::/10', 'ff00::/8', 'fd00:ec2::254/128']]
    result = []
    for item in value:
        if not isinstance(item, str) or '/' not in item:
            raise Invalid(f'{label}: require explicit CIDR notation')
        try:
            network = ipaddress.ip_network(item, strict=True)
        except (TypeError, ValueError):
            raise Invalid(f'{label}: require canonical CIDR notation') from None
        if network.prefixlen == 0 or any(network.version == b.version and network.overlaps(b) for b in blocked):
            raise Invalid(f'{label}: wildcard, local, multicast or metadata overlap is forbidden')
        result.append(str(network))
    if len(set(result)) != len(result):
        raise Invalid(f'{label}: duplicate destinations')
    return result


def port(value, label):
    if type(value) is not int or not 1 <= value <= 65535:
        raise Invalid(f'{label}: require a TCP port')
    return value


def image(value, label):
    string(value, label, r'[a-z0-9][a-z0-9.:-]*/[a-z0-9][a-z0-9._/-]*@sha256:[a-f0-9]{64}', 512)
    repository, digest = value.split('@')
    if '..' in repository or '//' in repository:
        raise Invalid(f'{label}: invalid repository')
    return {'repository': repository, 'digest': digest, 'tag': 'digest-pinned', 'pullPolicy': 'IfNotPresent'}


def validate(config):
    obj(config, ['schemaVersion', 'context', 'namespace', 'release', 'kubernetesVersion', 'publicUrl', 'images', 'database', 'oidc', 'ingress', 'network'], ['imagePullSecrets'])
    if type(config['schemaVersion']) is not int or config['schemaVersion'] != 1:
        raise Invalid('schemaVersion: expected 1')
    string(config['context'], 'context', r'[A-Za-z0-9][A-Za-z0-9._:/@-]*', 253)
    name(config['namespace'], 'namespace')
    if config['namespace'] == 'default' or config['namespace'].startswith('kube-'):
        raise Invalid('namespace: require a dedicated application namespace')
    name(config['release'], 'release', 40)
    string(config['kubernetesVersion'], 'kubernetesVersion', r'1\.(?:29|[3-9][0-9])\.[0-9]{1,3}', 16)
    origin = endpoint(config['publicUrl'], 'publicUrl', True)
    obj(config['images'], ['api', 'web'], label='images')
    images = {k: image(v, 'images.' + k) for k, v in config['images'].items()}
    db = obj(config['database'], ['name', 'migrationRole', 'runtimeRole', 'migrationSecret', 'runtimeSecret', 'caSecret', 'egressCidrs', 'port'], label='database')
    for key in ['name', 'migrationRole', 'runtimeRole']:
        string(db[key], 'database.' + key, r'[a-z][a-z0-9_]*', 63)
    if db['migrationRole'] == db['runtimeRole']:
        raise Invalid('database: migration and runtime roles must differ')
    for key in ['migrationSecret', 'runtimeSecret', 'caSecret']:
        secret(db[key], 'database.' + key)
    if db['migrationSecret']['name'] == db['runtimeSecret']['name']:
        raise Invalid('database: migration and runtime Secret names must differ')
    db_cidrs = cidrs(db['egressCidrs'], 'database.egressCidrs')
    db_port = port(db['port'], 'database.port')
    oidc = obj(config['oidc'], ['issuer', 'audience', 'clientId', 'scopes'], ['caSecret'], label='oidc')
    issuer = endpoint(oidc['issuer'], 'oidc.issuer')
    for key in ['audience', 'clientId']:
        string(oidc[key], 'oidc.' + key, r'[A-Za-z0-9][A-Za-z0-9._:/-]*', 253)
    if oidc['audience'] == oidc['clientId']:
        raise Invalid('oidc: API audience must differ from browser client ID')
    scopes = string(oidc['scopes'], 'oidc.scopes', r'[A-Za-z0-9._:/-]+(?: [A-Za-z0-9._:/-]+)*', 512).split(' ')
    if 'openid' not in scopes or len(set(scopes)) != len(scopes):
        raise Invalid('oidc.scopes: require openid and unique space-separated scopes')
    if 'caSecret' in oidc:
        secret(oidc['caSecret'], 'oidc.caSecret')
    ingress = obj(config['ingress'], ['className', 'tlsSecretName', 'controllerNamespace', 'controllerPodLabels'], label='ingress')
    for key in ['className', 'tlsSecretName', 'controllerNamespace']:
        name(ingress[key], 'ingress.' + key)
    controller_labels = labels(ingress['controllerPodLabels'], 'ingress.controllerPodLabels')
    network = obj(config['network'], ['httpsEgressCidrs', 'httpsEgressPorts', 'dnsNamespace', 'dnsPodLabels'], label='network')
    https_cidrs = cidrs(network['httpsEgressCidrs'], 'network.httpsEgressCidrs')
    ports = network['httpsEgressPorts']
    if not isinstance(ports, list) or not 1 <= len(ports) <= 8 or any(type(p) is not int for p in ports) or len(set(ports)) != len(ports):
        raise Invalid('network.httpsEgressPorts: require unique explicit ports')
    ports = [port(p, 'network.httpsEgressPorts') for p in ports]
    if (issuer.port or 443) not in ports:
        raise Invalid('network.httpsEgressPorts: must include OIDC issuer port')
    name(network['dnsNamespace'], 'network.dnsNamespace')
    dns_labels = labels(network['dnsPodLabels'], 'network.dnsPodLabels')
    pulls = config.get('imagePullSecrets', [])
    if not isinstance(pulls, list) or len(pulls) > 8:
        raise Invalid('imagePullSecrets: require up to 8 Secret names')
    for pull in pulls:
        name(pull, 'imagePullSecrets')
    values = {
        'fullnameOverride': config['release'],
        'imagePullSecrets': [{'name': n} for n in pulls],
        'oidc': dict(oidc),
        'api': {'replicas': 2, 'image': images['api']},
        'web': {'replicas': 2, 'image': images['web']},
        'postgres': {'mode': 'external', 'external': {'secretName': db['runtimeSecret']['name'], 'secretKey': db['runtimeSecret']['key'], 'caSecretName': db['caSecret']['name'], 'caSecretKey': db['caSecret']['key']}},
        'migrations': {'enabled': True, 'databaseSecret': dict(db['migrationSecret'])},
        'ingress': {'enabled': True, 'className': ingress['className'], 'host': origin.hostname, 'tls': {'secretName': ingress['tlsSecretName'], 'externallyTerminated': False}},
        'networkPolicy': {'enabled': True, 'webIngressFrom': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': ingress['controllerNamespace']}}, 'podSelector': {'matchLabels': controller_labels}}], 'dnsNamespace': network['dnsNamespace'], 'dnsPodLabels': dns_labels, 'httpsEgressCidrs': https_cidrs, 'httpsEgressPorts': ports, 'postgresEgressCidrs': db_cidrs, 'postgresPort': db_port},
    }
    return values


def duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Invalid('profile: duplicate JSON keys')
        result[key] = value
    return result


def secret_references(value):
    result = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in ['secretKeyRef', 'secretRef'] and isinstance(child, dict):
                result.add(child.get('name'))
            if key == 'secret' and isinstance(child, dict):
                result.add(child.get('secretName') or child.get('name'))
            if key == 'imagePullSecrets' and isinstance(child, list):
                result.update(v.get('name') for v in child if isinstance(v, dict))
            result.update(secret_references(child))
    elif isinstance(value, list):
        for child in value:
            result.update(secret_references(child))
    return result


def load(path):
    try:
        with path.open('rb') as stream:
            data = stream.read(MAX_INPUT + 1)
        if len(data) > MAX_INPUT:
            raise Invalid('profile: input exceeds 64 KiB')
        return json.loads(data, object_pairs_hook=duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError):
        raise Invalid('profile: unreadable or invalid JSON') from None


def render(helm, values, config):
    # Only the repository chart and fixed template flags. No DNS, plugins,
    # dependency downloads, post-renderer, kube credentials, or arbitrary flags.
    env = {k: v for k, v in os.environ.items() if k in ['PATH', 'SYSTEMROOT', 'TMPDIR']}
    with tempfile.TemporaryDirectory(prefix='dataslate-setup-') as tmp:
        env.update({'HOME': tmp, 'KUBECONFIG': os.devnull, 'HELM_PLUGINS': tmp + '/plugins', 'HELM_CACHE_HOME': tmp + '/cache', 'HELM_CONFIG_HOME': tmp + '/config', 'HELM_DATA_HOME': tmp + '/data'})
        value_path = Path(tmp) / 'values.yaml'
        value_path.write_text(yaml.safe_dump(values, sort_keys=False))
        with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
            try:
                result = subprocess.run([helm, 'template', config['release'], str(CHART), '--namespace', config['namespace'], '--kube-version', config['kubernetesVersion'], '-f', str(value_path)], env=env, stdout=out, stderr=err, timeout=30, check=False)
            except subprocess.TimeoutExpired:
                raise Invalid('Helm render timed out (30 seconds)') from None
            except OSError:
                raise Invalid('Helm executable unavailable') from None
            if result.returncode:
                raise Invalid('Helm rejected generated configuration; inspect trusted chart compatibility')
            if out.tell() > MAX_RENDER:
                raise Invalid('Helm render exceeds 4 MiB')
            out.seek(0)
            rendered = out.read().decode('utf-8')
    docs = [d for d in yaml.safe_load_all(rendered) if d]
    if not docs or any(d.get('kind') in ['Secret', 'Namespace', 'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding', 'CustomResourceDefinition'] for d in docs):
        raise Invalid('Rendered core has unexpected provisioning or privilege objects')
    migration = config['database']['migrationSecret']
    jobs = [d for d in docs if d.get('kind') == 'Job']
    if len(jobs) != 1:
        raise Invalid('Rendered core must contain exactly one migration job')
    for doc in docs:
        if doc.get('kind') == 'Deployment' and migration['name'] in secret_references(doc):
            raise Invalid('Migration Secret unexpectedly appears in a runtime deployment')
    return rendered


def prerequisites(config):
    db = config['database']
    refs = [{'name': db[k]['name'], 'keys': [db[k]['key']], 'purpose': k} for k in ['runtimeSecret', 'migrationSecret', 'caSecret']]
    refs.append({'name': config['ingress']['tlsSecretName'], 'keys': ['tls.crt', 'tls.key'], 'purpose': 'Ingress TLS certificate and key'})
    if 'caSecret' in config['oidc']:
        refs.append({'name': config['oidc']['caSecret']['name'], 'keys': [config['oidc']['caSecret']['key']], 'purpose': 'OIDC-only private CA'})
    refs.extend({'name': n, 'keys': ['.dockerconfigjson'], 'purpose': 'Registry pull authentication'} for n in config.get('imagePullSecrets', []))
    return {'schemaVersion': 1, 'qualification': 'not-performed', 'context': config['context'], 'namespace': config['namespace'], 'existingSecrets': refs, 'database': {'name': db['name'], 'migrationRole': db['migrationRole'], 'runtimeRole': db['runtimeRole'], 'schema': 'public', 'tls': 'verify-full', 'credentialIdentityVerified': False}, 'requires': ['New release and namespace, or explicit review of existing ownership', 'Reachable compatible PostgreSQL; dedicated database and distinct verified roles', 'OIDC public PKCE client and API audience with trusted claims', 'Existing ingress controller and TLS Secret; matching selectors', 'CNI enforcing NetworkPolicy; exact DNS and destination reachability', 'Registry images exist for node architecture, and provenance/vulnerability review', 'Backup/restore, failure, rotation and tenant isolation qualification'], 'disabledComponents': ['Spark execution', 'Trino SQL', 'Polaris catalog', 'publication', 'semantic search', 'Sylon', 'Airflow workflow']}


def runbook(config, output, helm):
    q = shlex.quote
    helm_cmd, python_cmd = q(helm), q(sys.executable)
    ctx, ns, rel = (q(config[k]) for k in ['context', 'namespace', 'release'])
    target = q(str(output.resolve()))
    db = config['database']
    return f'''# Review and install a new DataSlate core release

Generated offline. No cluster contacted; no images or credentials verified.
Run repository commands from the checked-out source revision used to build images.
Commands retain the trusted Helm executable and Python environment used to generate
this bundle. Keep those tools available; regenerate the bundle if they move.
See `deploy/quickstart.md` for identity, database, backup and optional-engine setup.
The generated profile uses a Secret-terminating ingress controller, an existing
external PostgreSQL database, and existing Secrets. It does not install engines.

1. Review all five output files. Have an operator provision the dedicated database,
   distinct owner/migrator and restricted runtime roles, ingress TLS and named Secrets
   from `prerequisites.json` using your secret manager. Never put values in the profile.
   Both database URLs must select `{db['name']}`, use the intended identity and
   `sslmode=verify-full`, and match the provided CA and server hostname.
2. Confirm namespace/release ownership. For a genuinely new namespace only:

```sh
kubectl --context {ctx} get namespace {ns}
kubectl --context {ctx} create -f {target}/namespace.yaml
{helm_cmd} list --kube-context {ctx} --namespace {ns} --all
```

`create` must stop if the namespace exists. Do not delete or adopt it automatically.
Provision the named Secrets in this namespace through your approved secret workflow.
The commands below have deliberate human review gates; they are not an installer script.

3. Inspect the real installation prerequisites (requires read-only cluster access):

```sh
{python_cmd} deploy/preflight/check.py --context {ctx} --namespace {ns} --release {rel} --values {target}/values.yaml --helm {helm_cmd} --format json > {target}/preflight.json
```

A report with unresolved checks is not approval to install.
CNI enforcement, TLS trust, actual role identities and
workload execution require separate proof. Read the report and resolve blockers.

4. Install this NEW release, then wait for its migration Job. Do not use `--wait`
   yet: the runtime principal intentionally lacks application grants until step 5.
   Do not use `--atomic` as a database rollback strategy.

```sh
{helm_cmd} install {rel} deploy/helm/dataslate --kube-context {ctx} --namespace {ns} -f {target}/values.yaml --timeout 10m
kubectl --context {ctx} --namespace {ns} wait --for=condition=complete job/{config['release']}-migrate-1 --timeout=10m
```

5. From an approved PostgreSQL administration environment, set up a libpq service
   named `dataslate-migration` selecting this database as the migration role,
   `sslmode=verify-full`, trusted `sslrootcert`, and an owner-only password file.
   Do not pass a password/connection URL on the command line. Apply grants:

```sh
PGSERVICE=dataslate-migration psql -X -v ON_ERROR_STOP=1 -v database={q(db['name'])} -v migration_role={q(db['migrationRole'])} -v runtime_role={q(db['runtimeRole'])} -f deploy/setup/runtime-role.sql
```

The SQL deliberately rejects role membership and unsafe privileges. It grants
existing public application tables CRUD and sequence usage, with a read-only
migration ledger. Rerun after every migration; failed checks roll back grants.
Distinct Secret names alone do not prove distinct database identities.

6. Wait for the deployments; inspect only sanitized events if they fail:

```sh
kubectl --context {ctx} --namespace {ns} rollout status deployment/{config['release']}-api --timeout=10m
kubectl --context {ctx} --namespace {ns} rollout status deployment/{config['release']}-web --timeout=10m
{python_cmd} deploy/scripts/verify-install.py --url {q(config['publicUrl'].rstrip('/'))}
```

Then complete real browser PKCE login, workspace setup, two-user authorization and
cross-workspace denial tests. Unauthenticated smoke success is not production acceptance.
Use a trusted public ingress certificate or add `--ca-file /secure/config/ingress-ca.pem`
to the verifier for your private ingress CA; never disable verification. This is
client trust for the workspace certificate, separate from `oidc.caSecret`.

## Upgrades and optional components

Back up and prove restore first. Review the new chart and images, run preflight on
ALL final overlay files, and verify the existing release is yours. Use an explicit
`{helm_cmd} upgrade` with the same context/namespace. The migration Job is named
`{config['release']}-migrate-REVISION`; read the release revision, wait for that Job,
rerun runtime-role.sql, then verify readiness. Never reuse the initial `-migrate-1`
wait for an upgrade. Database rollback requires a separate tested procedure.
Optional engines each require separate credentials, bindings, network review and
live acceptance. The core profile intentionally leaves them disabled.
'''


def generate(config_path, output, helm):
    config = load(config_path)
    values = validate(config)
    if output.exists():
        raise Invalid('Output directory already exists; choose a new review bundle path')
    rendered = render(helm, values, config)
    namespace = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': config['namespace'], 'labels': {'pod-security.kubernetes.io/enforce': 'restricted', 'pod-security.kubernetes.io/enforce-version': 'v' + '.'.join(config['kubernetesVersion'].split('.')[:2]), 'pod-security.kubernetes.io/audit': 'restricted', 'pod-security.kubernetes.io/warn': 'restricted'}}}
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    files = {'values.yaml': yaml.safe_dump(values, sort_keys=False), 'namespace.yaml': yaml.safe_dump(namespace, sort_keys=False), 'prerequisites.json': json.dumps(prerequisites(config), indent=2) + '\n', 'rendered-core.yaml': rendered, 'RUNBOOK.md': runbook(config, output, helm)}
    for filename, content in files.items():
        path = output / filename
        with path.open('x') as stream:
            stream.write(content)
        path.chmod(0o600)
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True, help='Non-secret JSON profile')
    parser.add_argument('--output', type=Path, required=True, help='New directory for review bundle')
    parser.add_argument('--helm', default='helm', help='Trusted Helm executable; only offline template is called')
    args = parser.parse_args()
    try:
        generate(args.config, args.output, args.helm)
    except (Invalid, OSError, yaml.YAMLError, UnicodeError, TypeError, RecursionError) as exc:
        # Never include user-controlled values, subprocess stderr, or credentials.
        message = str(exc) if isinstance(exc, Invalid) else 'Input or output could not be processed safely'
        parser.exit(2, 'Setup failed: ' + message + '\n')
    print('Created five review files. No cluster contacted; installation and qualification remain explicit steps.')


if __name__ == '__main__':
    main()

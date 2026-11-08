"""Render private TLS/JWT files for the disposable local lake probe; no services.

Only the runtime/ subdirectory is mounted into Trino. The signing keys and API
tokens remain outside that mount. No cloud credentials or user data are used.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

POLICY_ROOT = Path(__file__).resolve().parents[2]/'deploy'/'sql'
sys.path.insert(0, str(POLICY_ROOT))
from policy import compile_rules


def prepare(root):
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError('Use an absolute clean path to a new private directory')
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    previous = os.umask(0o077)
    try:
        def run(args):
            return subprocess.run(args, check=True, capture_output=True).stdout

        def save(name, value):
            with (root/name).open('xb') as stream:
                stream.write(value.encode() if isinstance(value, str) else value)

        save('ownership.json', json.dumps({'profile': 'dataslate-lake-tls-v1', 'synthetic': True}))
        run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(root/'ca.key'), '-out', str(root/'ca.crt'), '-days', '2', '-subj', '/CN=DataSlate local lake CA', '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign'])
        run(['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(root/'tls.key'), '-out', str(root/'tls.csr'), '-subj', '/CN=localhost'])
        save('tls.ext', 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n')
        run(['openssl', 'x509', '-req', '-in', str(root/'tls.csr'), '-CA', str(root/'ca.crt'), '-CAkey', str(root/'ca.key'), '-CAcreateserial', '-out', str(root/'tls.crt'), '-days', '2', '-extfile', str(root/'tls.ext')])
        save('tls.pem', (root/'tls.key').read_bytes()+(root/'tls.crt').read_bytes())
        run(['openssl', 'genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', str(root/'jwt-private.pem')])
        save('jwt-key.pem', run(['openssl', 'pkey', '-in', str(root/'jwt-private.pem'), '-pubout']))

        def b64(value):
            return base64.urlsafe_b64encode(value).rstrip(b'=')

        for user in ['dataslate-broker', 'dataslate-publisher', 'team-alice', 'team-bob']:
            claims = {'sub': user, 'iss': 'https://dataslate.invalid/lake-qualification', 'aud': 'dataslate-trino', 'iat': int(time.time())-10, 'exp': int(time.time())+7200}
            message = b64(json.dumps({'alg': 'RS256', 'typ': 'JWT'}).encode())+b'.'+b64(json.dumps(claims).encode())
            signature = subprocess.run(['openssl', 'dgst', '-sha256', '-sign', str(root/'jwt-private.pem')], input=message, check=True, capture_output=True).stdout
            save(user+'.token', message+b'.'+b64(signature))
        profile = {'brokerUser': 'dataslate-broker', 'grants': [{'user': 'team-alice', 'catalog': 'lake', 'schema': 'default', 'table': 'orders', 'rowFilter': "region = 'US'", 'columns': [{'name': 'email', 'mask': "CAST('REDACTED' AS VARCHAR)"}]}],
                   'publishers': [{'user': 'dataslate-publisher', 'catalog': 'lake', 'schema': 'default', 'table': table} for table in ['orders', 'timestamp_precision']]}
        save('profile.json', json.dumps(profile, indent=2))
        save('policy-source.json', json.dumps({'source': 'deploy/sql/policy.py', 'sha256': hashlib.sha256((POLICY_ROOT/'policy.py').read_bytes()).hexdigest()}))
        (root/'runtime').mkdir(); (root/'runtime'/'catalog').mkdir()
        save('runtime/rules.json', json.dumps(compile_rules(profile), indent=2))
        for name in ['tls.pem', 'ca.crt', 'jwt-key.pem']:
            save('runtime/'+name, (root/name).read_bytes())
        save('runtime/access-control.properties', 'access-control.name=file\nsecurity.config-file=/etc/trino/rules.json\n')
        save('runtime/node.properties', 'node.environment=qualification\nnode.id=dataslate-lake-coordinator\nnode.data-dir=/data/trino\nnode.internal-address=127.0.0.1\n')
        save('runtime/jvm.config', '-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n-XX:ReservedCodeCacheSize=256M\n-Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n')
        save('runtime/config.properties', 'coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.enabled=false\nhttp-server.https.enabled=true\nhttp-server.https.port=8443\nhttp-server.https.keystore.path=/etc/trino/tls.pem\nhttp-server.authentication.type=JWT\nhttp-server.authentication.jwt.key-file=/etc/trino/jwt-key.pem\nhttp-server.authentication.jwt.required-issuer=https://dataslate.invalid/lake-qualification\nhttp-server.authentication.jwt.required-audience=dataslate-trino\nhttp-server.authentication.allow-insecure-over-http=false\ninternal-communication.shared-secret='+secrets.token_urlsafe(64)+'\ninternal-communication.https.required=true\ninternal-communication.https.truststore.path=/etc/trino/ca.crt\ndiscovery.uri=https://localhost:8443\nquery.max-memory=512MB\nquery.max-memory-per-node=256MB\nmemory.heap-headroom-per-node=256MB\nquery.max-run-time=2m\n')
        # Trino484's actual File configuration expects a filesystem path here.
        # The location URI supplied to register_table remains file:///lake/... .
        save('runtime/catalog/lake.properties', 'connector.name=delta_lake\nhive.metastore=thrift\nhive.metastore.uri=thrift://dataslate-lake-hms:9083\nfs.local.enabled=true\nlocal.location=/\ndelta.security=SYSTEM\ndelta.register-table-procedure.enabled=true\n')
    finally:
        os.umask(previous)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--directory', type=Path, required=True)
    prepare(parser.parse_args().directory)
    print('Private local qualification configuration prepared; no services started and no credentials printed.')

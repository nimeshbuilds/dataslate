#!/usr/bin/env python3
"""Prepare, but never apply, a private fixture for an owned local cluster."""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import yaml

from render import render, ROOT

NAMESPACE = 'dataslate-sql-qualification'


def prepare(directory, helm, chart):
    root = Path(directory).resolve()
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    # This helper never overwrites an existing fixture or contacts Kubernetes.
    def run(args):
        return subprocess.run(args, check=True, capture_output=True).stdout

    def save(name, data):
        path = root / name
        with path.open('xb') as handle:
            os.chmod(path, 0o600)
            handle.write(data if isinstance(data, bytes) else data.encode())

    # OpenSSL writes private material; umask protects files before chmod.
    previous = os.umask(0o077)
    try:
        run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(root/'ca.key'),
             '-out', str(root/'ca.crt'), '-days', '2', '-subj', '/CN=DataSlate disposable SQL CA',
             '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign'])
        run(['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(root/'tls.key'),
             '-out', str(root/'tls.csr'), '-subj', '/CN=dataslate-trino.'+NAMESPACE+'.svc'])
        save('tls.ext', 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n'
             'extendedKeyUsage=serverAuth\nsubjectAltName=DNS:dataslate-trino.'+NAMESPACE+'.svc,'
             'DNS:*.dataslate-trino-nodes.'+NAMESPACE+'.svc,DNS:localhost,IP:127.0.0.1\n')
        run(['openssl', 'x509', '-req', '-in', str(root/'tls.csr'), '-CA', str(root/'ca.crt'),
             '-CAkey', str(root/'ca.key'), '-CAcreateserial', '-out', str(root/'tls.crt'), '-days', '2',
             '-extfile', str(root/'tls.ext')])
        save('tls.pem', (root/'tls.key').read_bytes()+(root/'tls.crt').read_bytes())
        run(['openssl', 'genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', str(root/'jwt-private.pem')])
        run(['openssl', 'pkey', '-in', str(root/'jwt-private.pem'), '-pubout', '-out', str(root/'jwt-key.pem')])
        def b64(value): return base64.urlsafe_b64encode(value).rstrip(b'=')
        claims = {'sub':'dataslate-broker','iss':'https://dataslate.invalid/qualification',
                  'aud':'dataslate-trino','iat':int(time.time())-10,'exp':int(time.time())+7200}
        message = b64(json.dumps({'alg':'RS256','typ':'JWT'}).encode())+b'.'+b64(json.dumps(claims).encode())
        save('signing-input', message)
        signature = run(['openssl','dgst','-sha256','-sign',str(root/'jwt-private.pem'),str(root/'signing-input')])
        save('token', message+b'.'+b64(signature))
        profile = json.loads((ROOT/'profile.example.json').read_text())
        profile.update(namespace=NAMESPACE, workers=1, jwtIssuer=claims['iss'],
                       controllerNamespace=NAMESPACE, controllerLabels={'dataslate.io/qualification':'sql'}, egress=[],
                       grants=[{'user':'team-alice','catalog':'tpch','schema':'tiny','table':'nation',
                                'rowFilter':'regionkey = 1','columns':[{'name':'name','mask':"CAST('REDACTED' AS VARCHAR(25))"}]}])
        save('profile.json', json.dumps(profile, indent=2))
        def secret(name, values):
            return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':NAMESPACE},
                    'type':'Opaque','data':{key:base64.b64encode(value).decode() for key,value in values.items()}}
        prerequisites = [
            {'apiVersion':'v1','kind':'Namespace','metadata':{'name':NAMESPACE,'labels':{'dataslate.io/qualification':'sql'}}},
            {'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':profile['serviceAccount'],'namespace':NAMESPACE},'automountServiceAccountToken':False},
            secret(profile['tlsSecret'],{'tls.pem':(root/'tls.pem').read_bytes(),'ca.crt':(root/'ca.crt').read_bytes()}),
            secret(profile['authSecret'],{'jwt-key.pem':(root/'jwt-key.pem').read_bytes(),'internal-shared-secret':secrets.token_urlsafe(96).encode()}),
            secret(profile['catalogSecret'],{'tpch.properties':b'connector.name=tpch\n'}),
        ]
        save('prerequisites.yaml', yaml.safe_dump_all(prerequisites,sort_keys=False))
        save('server.yaml', yaml.safe_dump_all(render(profile,helm,chart),sort_keys=False))
    finally:
        os.umask(previous)
    return root


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='New private directory; parent must exist')
    parser.add_argument('--helm', required=True)
    parser.add_argument('--chart', required=True)
    args = parser.parse_args()
    prepare(args.directory,args.helm,args.chart)
    print('Private fixture prepared; no cluster changes made and no credential values printed.')

#!/usr/bin/env python3
"""Prepare a disposable local-only Polaris/PostgreSQL fixture. Does not apply it.

Writes credentials with mode 0600 under a new directory. FILE storage and ephemeral
PostgreSQL are qualification overrides, never the managed production profile.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import yaml
import render

NAMESPACE = 'dataslate-polaris-qualification'
POSTGRES = 'postgres:17@sha256:ff87138949ad9a46f32ff23bd6852b52ce024a13dffe9b4087be98292cdd3b4f'


def prepare(directory, helm, chart):
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    def save(name, value):
        path=directory/name
        with path.open('x') as f:
            os.chmod(path,0o600);f.write(value)
        return path
    config=json.loads((Path(__file__).parent/'profile.example.json').read_text())
    config.update(name='polaris',namespace=NAMESPACE,replicas=1,serviceAccount='polaris',clientNamespace=NAMESPACE,clientLabels={'dataslate.io/polaris-client':'true'},egress=[{'cidr':'10.42.0.0/16','port':5432}])
    password=secrets.token_urlsafe(32);clientsecret=secrets.token_urlsafe(40)
    creds={'DATASLATE':{'client-id':'qualification-root','client-secret':clientsecret}}
    save('client-secret',clientsecret);save('bootstrap-credentials.json',json.dumps(creds))
    tlsconf=save('openssl.cnf','[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=polaris\n[ext]\nsubjectAltName=DNS:localhost,DNS:polaris,DNS:polaris.'+NAMESPACE+'.svc,IP:127.0.0.1\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,digitalSignature,keyEncipherment,keyCertSign\n')
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-sha256','-days','2','-keyout',str(directory/'tls.key'),'-out',str(directory/'tls.crt'),'-config',str(tlsconf)],check=True,capture_output=True)
    subprocess.run(['openssl','genpkey','-algorithm','RSA','-pkeyopt','rsa_keygen_bits:2048','-out',str(directory/'private.pem')],check=True,capture_output=True)
    subprocess.run(['openssl','pkey','-in',str(directory/'private.pem'),'-pubout','-out',str(directory/'public.pem')],check=True,capture_output=True)
    for p in directory.iterdir():os.chmod(p,0o600)
    def secret(name,values):return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':NAMESPACE},'type':'Opaque','data':{k:base64.b64encode(v if isinstance(v,bytes) else v.encode()).decode() for k,v in values.items()}}
    meta={'namespace':NAMESPACE,'name':'postgres'}
    base=[{'apiVersion':'v1','kind':'Namespace','metadata':{'name':NAMESPACE,'labels':{'dataslate.io/qualification':'polaris','pod-security.kubernetes.io/enforce':'restricted'}}},
        {'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':'polaris','namespace':NAMESPACE},'automountServiceAccountToken':False},
        secret(config['databaseSecret'],{'username':'polaris','password':password,'jdbcUrl':'jdbc:postgresql://postgres:5432/polaris'}),
        secret(config['rootCredentialsSecret'],{'credentials.json':json.dumps(creds)}),
        secret(config['tlsSecret'],{k:(directory/k).read_bytes() for k in ('tls.crt','tls.key')}),
        secret(config['tokenKeySecret'],{k:(directory/k).read_bytes() for k in ('public.pem','private.pem')}),
        {'apiVersion':'v1','kind':'ConfigMap','metadata':{**meta,'name':'postgres-init'},'data':{'init.sql':'CREATE SCHEMA polaris_schema AUTHORIZATION polaris;'}},
        {'apiVersion':'v1','kind':'Service','metadata':meta,'spec':{'selector':{'app':'postgres'},'ports':[{'port':5432,'targetPort':5432}]}},
        {'apiVersion':'apps/v1','kind':'Deployment','metadata':meta,'spec':{'replicas':1,'selector':{'matchLabels':{'app':'postgres'}},'template':{'metadata':{'labels':{'app':'postgres'}},'spec':{'automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':999,'fsGroup':999,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'postgres','image':POSTGRES,'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'env':[{'name':'POSTGRES_USER','value':'polaris'},{'name':'POSTGRES_DB','value':'polaris'},{'name':'PGDATA','value':'/var/lib/postgresql/data/pgdata'},{'name':'POSTGRES_PASSWORD','valueFrom':{'secretKeyRef':{'name':config['databaseSecret'],'key':'password'}}}], 'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'cpu':'1','memory':'256Mi'}},'ports':[{'containerPort':5432}],'readinessProbe':{'exec':{'command':['pg_isready','-U','polaris','-d','polaris']},'initialDelaySeconds':3,'periodSeconds':3},'volumeMounts':[{'name':'data','mountPath':'/var/lib/postgresql/data'},{'name':'init','mountPath':'/docker-entrypoint-initdb.d','readOnly':True}]}],'volumes':[{'name':'data','emptyDir':{'sizeLimit':'1Gi'}},{'name':'init','configMap':{'name':'postgres-init'}}]}}}},
    ]
    docs=render.render(config,helm,chart)
    save('production-server.yaml',yaml.safe_dump_all([d for d in docs if d['kind'] not in ('Job','NetworkPolicy')],sort_keys=False))
    for d in docs:
        if d['kind']=='ConfigMap':
            prop=d.get('data',{}).get('application.properties','')
            if 'SUPPORTED_CATALOG_STORAGE_TYPES' in prop:
                # Explicit local fixture only; production renderer never enables FILE.
                import re
                d['data']['application.properties']=re.sub(r'^polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=.*$', 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE"]',prop,flags=re.M)
                d['data']['application.properties']+='\npolaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true\npolaris.readiness.ignore-severe-issues=true\n'
        if d['kind']=='Deployment':
            d['spec']['strategy']={'type':'Recreate'}
            pod=d['spec']['template']['spec'];pod['volumes'].append({'name':'fixture-lake','emptyDir':{'sizeLimit':'512Mi'}})
            pod['containers'][0]['volumeMounts'].append({'name':'fixture-lake','mountPath':'/fixture-lake'})
            d['spec']['template']['metadata'].setdefault('annotations',{})['dataslate.io/qualification-profile']='file-storage-only'
    bootstrap=[d for d in docs if d['kind'] in ('Job','NetworkPolicy')]
    server=[d for d in docs if d['kind'] not in ('Job','NetworkPolicy')]
    save('prerequisites.yaml',yaml.safe_dump_all(base,sort_keys=False));save('bootstrap.yaml',yaml.safe_dump_all(bootstrap,sort_keys=False));save('server.yaml',yaml.safe_dump_all(server,sort_keys=False))
    save('profile.json',json.dumps(config,indent=2))
    save('client.json',json.dumps({'endpoint':'https://localhost:28443/api/catalog','warehouse':'qualification','realm':'DATASLATE','caFile':str((directory/'tls.crt').absolute()),'auth':{'clientId':'qualification-root','clientSecretFile':str((directory/'client-secret').absolute()),'tokenEndpoint':'https://localhost:28443/api/catalog/v1/oauth/tokens','scope':'PRINCIPAL_ROLE:ALL'}},indent=2))
    print(json.dumps({'prepared':str(directory),'namespace':NAMESPACE,'applied':False,'fixtureOnly':True}))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--directory',required=True);p.add_argument('--helm',required=True);p.add_argument('--chart',required=True);a=p.parse_args();prepare(Path(a.directory),a.helm,a.chart)

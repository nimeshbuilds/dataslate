#!/usr/bin/env python3
"""Render the pinned Trino chart with DataSlate's governed SQL profile.

Does not contact or mutate Kubernetes. The operator reserves a Service ClusterIP
and provisions referenced Secrets before applying the reviewed output. Uses the
upstream chart, then performs checked adaptations for internal TLS and credentials.
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
from policy import compile_rules

ROOT=Path(__file__).resolve().parent

def name(value):
    if not isinstance(value,str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,48}[a-z0-9]',value):
        raise ValueError('Expected a 2–50 character Kubernetes name')
    return value

def settings(config):
    required={'name','namespace','serviceIP','tlsSecret','authSecret','catalogSecret','jwtIssuer','jwtAudience','brokerUser','grants','workers','controllerNamespace','controllerLabels','egress','serviceAccount'}
    if not isinstance(config,dict) or set(config)!=required: raise ValueError('Missing or unknown profile fields')
    for key in ('name','namespace','tlsSecret','authSecret','catalogSecret','controllerNamespace','serviceAccount'): name(config[key])
    ip=ipaddress.ip_address(config['serviceIP'])
    if ip.is_unspecified or ip.is_multicast or ip.is_loopback or not ip.is_private: raise ValueError('serviceIP must be a reserved private Service IP')
    if type(config['workers']) is not int or not 0<=config['workers']<=32: raise ValueError('workers must be 0..32')
    for key in ('jwtIssuer','jwtAudience'):
        if not isinstance(config[key],str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:/_.@-]{0,511}',config[key]): raise ValueError('Invalid JWT issuer/audience')
    labels=config['controllerLabels']
    if not isinstance(labels,dict) or not labels or any(not isinstance(k,str) or not isinstance(v,str) or not re.fullmatch(r'[A-Za-z0-9_./-]{1,253}',k) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}',v) for k,v in labels.items()): raise ValueError('Explicit SQL controller labels are required')
    egress=config['egress']
    if not isinstance(egress,list) or len(egress)>100: raise ValueError('Bounded egress list required')
    network=[]
    for item in egress:
        if not isinstance(item,dict) or set(item)!={'cidr','port'} or type(item['port']) is not int or not 1<=item['port']<=65535: raise ValueError('Each egress requires cidr and port')
        subnet=ipaddress.ip_network(item['cidr'],strict=True)
        if subnet.prefixlen==0: raise ValueError('Unrestricted egress is not accepted')
        network.append({'to':[{'ipBlock':{'cidr':str(subnet)}}],'ports':[{'protocol':'TCP','port':item['port']}]})
    rules=compile_rules({'brokerUser':config['brokerUser'],'grants':config['grants']})
    image=json.loads((ROOT/'upstream-lock.json').read_text())['image']
    values={
        'fullnameOverride':config['name'],'image':{'repository':'trinodb/trino','digest':image.split('@')[1]},
        'serviceAccount':{'create':False,'name':config['serviceAccount']},
        'server':{'workers':config['workers'],'config':{'authenticationType':'JWT','query':{'maxMemory':'512MB'},'https':{'enabled':True,'port':8443,'keystore':{'path':'/etc/trino/tls/tls.pem'}}}},
        'catalogs':{'tpch':None,'tpcds':None},
        'additionalConfigProperties':[
            'internal-communication.shared-secret=${ENV:TRINO_INTERNAL_SECRET}',
            'internal-communication.https.required=true',
            'internal-communication.https.truststore.path=/etc/trino/tls/ca.crt',
            'http-server.authentication.jwt.key-file=/etc/trino/auth/jwt-key.pem',
            'http-server.authentication.jwt.required-issuer='+config['jwtIssuer'],
            'http-server.authentication.jwt.required-audience='+config['jwtAudience'],
            'http-server.authentication.allow-insecure-over-http=false',
            'query.max-run-time=1h','query.max-execution-time=1h',
        ],
        'additionalNodeProperties':['node.internal-address=${ENV:TRINO_NODE_DNS}'],
        'accessControl':{'type':'configmap','rules':{'rules.json':json.dumps(rules)},'configFile':'rules.json'},
        'env':[{'name':'AWS_EC2_METADATA_DISABLED','value':'true'}, {'name':'TRINO_POD_NAME','valueFrom':{'fieldRef':{'fieldPath':'metadata.name'}}},
               {'name':'TRINO_NODE_DNS','value':'$(TRINO_POD_NAME).'+config['name']+'-nodes.'+config['namespace']+'.svc'},
               {'name':'TRINO_INTERNAL_SECRET','valueFrom':{'secretKeyRef':{'name':config['authSecret'],'key':'internal-shared-secret'}}}],
        'secretMounts':[{'name':'sql-tls','secretName':config['tlsSecret'],'path':'/etc/trino/tls'},
                        {'name':'sql-auth','secretName':config['authSecret'],'path':'/etc/trino/auth'},
                        {'name':'sql-catalogs','secretName':config['catalogSecret'],'path':'/etc/trino/catalog'}],
        'securityContext':{'runAsUser':1000,'runAsGroup':1000,'runAsNonRoot':True,'seccompProfile':{'type':'RuntimeDefault'}},
        'containerSecurityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
        'coordinator':{'jvm':{'maxHeapSize':'1G'},'config':{'nodeScheduler':{'includeCoordinator':config['workers']==0},'query':{'maxMemoryPerNode':'256MB'},'memory':{'heapHeadroomPerNode':'256MB'}},'resources':{'requests':{'cpu':'500m','memory':'2Gi'},'limits':{'cpu':'2','memory':'3Gi'}}},
        'worker':{'jvm':{'maxHeapSize':'2G'},'config':{'query':{'maxMemoryPerNode':'512MB'},'memory':{'heapHeadroomPerNode':'512MB'}},'resources':{'requests':{'cpu':'1','memory':'3Gi'},'limits':{'cpu':'4','memory':'4Gi'}}},
        'networkPolicy':{'enabled':True,'ingress':[{'from':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':config['controllerNamespace']}},'podSelector':{'matchLabels':labels}}],'ports':[{'protocol':'TCP','port':8443}]}],'egress':[
            {'to':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'kube-system'}},'podSelector':{'matchLabels':{'k8s-app':'kube-dns'}}}],'ports':[{'protocol':'UDP','port':53},{'protocol':'TCP','port':53}]},
            {'to':[{'podSelector':{'matchLabels':{'app.kubernetes.io/instance':config['name'],'app.kubernetes.io/name':'trino'}}}],'ports':[{'protocol':'TCP','port':8443}]},*network]},
    }
    return values

def adapt(documents, config):
    docs=copy.deepcopy(documents)
    counters={'service':0,'coordinator':0,'worker':0}
    address=config['name']+'.'+config['namespace']+'.svc'
    for doc in docs:
        if not doc: continue
        meta=doc.setdefault('metadata',{})
        if meta.get('namespace')!=config['namespace']: raise ValueError('Unexpected namespace in upstream chart')
        meta.setdefault('labels',{})['app.kubernetes.io/managed-by']='DataSlate'
        meta.setdefault('annotations',{})['dataslate.io/upstream-profile']='trino-484-chart-1.45.0'
        if doc['kind']=='Service' and meta['name']==config['name']:
            doc['spec']['clusterIP']=config['serviceIP'];counters['service']+=1
            # Expose only TLS to other clients. Pod loopback HTTP remains for upstream probes.
            doc['spec']['ports']=[p for p in doc['spec']['ports'] if p['name']=='https']
            if len(doc['spec']['ports'])!=1: raise ValueError('Expected upstream HTTPS service port')
        if doc['kind']=='ConfigMap' and 'config.properties' in (doc.get('data') or {}):
            props=doc['data']['config.properties'];role='coordinator' if 'coordinator=true\n' in props else 'worker'
            if len(re.findall(r'^discovery.uri=',props,re.M))!=1: raise ValueError('Unexpected discovery configuration')
            props=re.sub(r'^discovery.uri=.*$',f'discovery.uri=https://{address}:8443',props,flags=re.M)
            if role=='worker': props+='\nhttp-server.authentication.type=JWT\nhttp-server.https.enabled=true\nhttp-server.https.port=8443\nhttp-server.https.keystore.path=/etc/trino/tls/tls.pem\n'
            doc['data']['config.properties']=props;counters[role]+=1
        if doc['kind']=='ServiceAccount': doc['automountServiceAccountToken']=False
        if doc['kind']=='Deployment':
            # Stable per-pod DNS permits verified internal TLS with an ordinary
            # cluster CA. Automatic Trino certificates exclude Service IPs.
            doc['kind']='StatefulSet'
            doc['spec']['serviceName']=config['name']+'-nodes'
            doc['spec'].pop('strategy',None)
            doc['spec'].pop('progressDeadlineSeconds',None)
            doc['spec']['updateStrategy']={'type':'RollingUpdate'}
            pod=doc['spec']['template']['spec'];pod['automountServiceAccountToken']=False
            c=pod['containers'][0]
            for mount in c['volumeMounts']: mount['readOnly']=True
            if len({v['mountPath'] for v in c['volumeMounts']})!=len(c['volumeMounts']): raise ValueError('Overlapping volume mounts')
            for vol,path,size in [('trino-data','/data/trino','4Gi'),('trino-tmp','/tmp','1Gi')]:
                pod['volumes'].append({'name':vol,'emptyDir':{'sizeLimit':size}})
                c['volumeMounts'].append({'name':vol,'mountPath':path})
            if not any(p['containerPort']==8443 for p in c['ports']): c['ports'].append({'name':'https','containerPort':8443,'protocol':'TCP'})
            # Checked adaptations alter configuration after Helm computed checksums.
            doc['spec']['template']['metadata'].setdefault('annotations',{})['dataslate.io/profile-sha256']=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
    if counters!={'service':1,'coordinator':1,'worker':int(config['workers']>0)}: raise ValueError('Unexpected pinned chart resource set')
    docs.append({'apiVersion':'v1','kind':'Service','metadata':{'name':config['name']+'-nodes','namespace':config['namespace']},'spec':{'clusterIP':'None','publishNotReadyAddresses':True,'selector':{'app.kubernetes.io/instance':config['name'],'app.kubernetes.io/name':'trino'},'ports':[{'name':'https','port':8443,'targetPort':'https','protocol':'TCP'}]}})
    return docs

def render(config,helm,chart):
    values=settings(config)
    lock=json.loads((ROOT/'upstream-lock.json').read_text())
    if hashlib.sha256(Path(chart).read_bytes()).hexdigest()!=lock['sha256']: raise ValueError('Upstream chart checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='dataslate-sql-render-') as d:
        path=Path(d)/'values.json';path.write_text(json.dumps(values))
        output=subprocess.run([helm,'template',config['name'],str(chart),'--namespace',config['namespace'],'--skip-tests','-f',str(path)],capture_output=True,text=True,check=True).stdout
    if not output.strip(): raise ValueError('Helm returned no manifests')
    return adapt(list(yaml.safe_load_all(output)),config)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--helm',required=True);parser.add_argument('--chart',required=True)
    args=parser.parse_args()
    print(yaml.safe_dump_all(render(json.loads(Path(args.config).read_text()),args.helm,args.chart),sort_keys=False))

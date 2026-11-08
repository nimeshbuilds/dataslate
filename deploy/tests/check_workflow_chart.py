"""Workflow broker/read credential separation and current metadata authority."""
import copy
import json
from pathlib import Path
import tempfile
import yaml


def workflow_values():
    return {'workflow':{'enabled':True,'bindings':[{
        'id':'orders','workspaceId':'default','name':'Orders daily','policyVersion':'v1',
        'endpoint':'https://airflow.example.com:8443','credentialsId':'orders',
        'dagId':'orders_daily','dagVersion':3,'bundleVersion':'a'*40,'airflowVersion':'3.4.0',
        'metadataAccessAccepted':True,'maxRunSeconds':3600,
        'allowedSubjects':[{'issuer':'https://identity.example.com/realms/dataslate','subject':'engineer-123'}]}],
        'credentials':[{'id':'orders','readTokenSecret':{'name':'airflow-reader','key':'token'},
                        'triggerTokenSecret':{'name':'airflow-trigger','key':'token'},'caSecret':{'name':'airflow-ca','key':'ca.crt'}}],
        'engineEgress':[{'cidr':'10.30.0.14/32','port':8443}]}}


def check_workflow_chart(run,external_args):
    values=workflow_values()
    with tempfile.TemporaryDirectory(prefix='dataslate-workflow-chart-') as directory:
        path=Path(directory)/'values.json';args=[*external_args,'-f',path]
        def render(config,error=None):
            path.write_text(json.dumps(config));out=run(args,error)
            return {(d['kind'],d['metadata']['name']):d for d in yaml.safe_load_all(out) if d} if out is not None else {}
        docs=render(values)
        config=json.loads(docs['ConfigMap','dataslate-workflow']['data']['bindings.json'])
        expected=copy.deepcopy(values['workflow']['bindings'][0]);del expected['credentialsId']
        expected.update(tokenFile='/etc/dataslate/workflow-credentials/orders/token',caFile='/etc/dataslate/workflow-credentials/orders/ca.crt')
        assert config==[expected]
        worker=docs['Deployment','dataslate-workflow-controller']['spec']['template'];pod=worker['spec'];container=pod['containers'][0]
        assert pod['automountServiceAccountToken'] is False and 'dataslate.io/tier' not in worker['metadata']['labels']
        assert pod['securityContext']['fsGroup']==65532
        assert set(worker['metadata']['annotations'])=={'checksum/workflow'}
        assert container['securityContext']['readOnlyRootFilesystem'] is True
        assert container['securityContext']['capabilities']['drop']==['ALL']
        assert container['ports']==[{'name':'http','containerPort':8086,'protocol':'TCP'}]
        env={v['name']:v for v in container['env']}
        assert set(env)=={'DATASLATE_WORKFLOW_CONTROLLER_ADDR','DATASLATE_WORKFLOW_BINDINGS_FILE','DATASLATE_DATABASE_URL','PGSSLMODE','PGSSLROOTCERT'}
        assert env['PGSSLMODE']['value']=='verify-full'
        assert 'airflow-reader' not in json.dumps(pod) and 'airflow-trigger' in json.dumps(pod)
        api=docs['Deployment','dataslate-api']['spec']['template']['spec']
        assert 'airflow-trigger' not in json.dumps(api) and 'airflow-reader' in json.dumps(api)
        assert api['securityContext']['fsGroup']==65532
        for source in (api,pod):
            volumes={v['name']:v for v in source['volumes']};projected=volumes['workflow-credential-orders']['projected']
            assert projected['defaultMode']==0o440 and len(projected['sources'])==2
            assert all('subPath' not in m for m in source['containers'][0]['volumeMounts'])
        for (kind,name),d in docs.items():
            if kind in ('Role','RoleBinding','ClusterRole','ClusterRoleBinding'):assert 'workflow-controller' not in json.dumps(d)
        policy=docs['NetworkPolicy','dataslate-workflow-controller']['spec']
        assert policy['ingress']==[] and len(policy['egress'])==3 and '0.0.0.0/0' not in json.dumps(policy)
        assert policy['egress'][-1]=={'to':[{'ipBlock':{'cidr':'10.30.0.14/32'}}],'ports':[{'protocol':'TCP','port':8443}]}
        assert docs['NetworkPolicy','dataslate-workflow-metadata']['spec']['podSelector']['matchLabels']['app.kubernetes.io/component']=='api'
        revoked=copy.deepcopy(values);revoked['workflow']['bindings']=[];after=render(revoked)
        assert json.loads(after['ConfigMap','dataslate-workflow']['data']['bindings.json'])==[]
        assert after['Deployment','dataslate-workflow-controller']['spec']['template']['spec']==pod
        cases=[
            (['workflow','enabled'],False,'workflow.enabled'),
            (['workflow','engineEgress'],[],'workflow.engineEgress'),
            (['workflow','credentials'],[],'credentialsId'),
            (['workflow','credentials',0,'readTokenSecret','name'],'airflow-trigger','distinct Secret'),
            (['workflow','credentials',0,'triggerTokenSecret','value'],'secret','value'),
            (['workflow','credentials',0,'triggerTokenSecret','key'],'../token','key'),
            (['workflow','bindings',0,'tokenFile'],'/tmp/token','tokenFile'),
            (['workflow','bindings',0,'caFile'],'/tmp/ca','caFile'),
            (['workflow','bindings',0,'endpoint'],'http://airflow','endpoint'),
            (['workflow','bindings',0,'endpoint'],'https://airflow/api/v2','endpoint'),
            (['workflow','bindings',0,'endpoint'],'https://user:pass@airflow','endpoint'),
            (['workflow','bindings',0,'dagId'],'../dag','dagId'),
            (['workflow','bindings',0,'dagVersion'],0,'dagVersion'),
            (['workflow','bindings',0,'dagVersion'],True,'dagVersion'),
            (['workflow','bindings',0,'bundleVersion'],'','bundleVersion'),
            (['workflow','bindings',0,'airflowVersion'],'2.11.0','airflowVersion'),
            (['workflow','bindings',0,'metadataAccessAccepted'],False,'metadataAccessAccepted'),
            (['workflow','bindings',0,'conf'],{'secret':'hidden'},'conf'),
            (['workflow','bindings',0,'maxRunSeconds'],59,'maxRunSeconds'),
            (['workflow','bindings',0,'maxRunSeconds'],86401,'maxRunSeconds'),
            (['workflow','bindings',0,'name'],' ','must not be blank'),
            (['workflow','bindings',0,'allowedSubjects',0,'issuer'],'https://other.example','issuers must match'),
            (['workflow','bindings',0,'allowedSubjects',0,'subject'],' ','subjects must not be blank'),
            (['workflow','engineEgress',0,'cidr'],'0.0.0.0/0','cidr'),
            (['workflow','engineEgress',0,'cidr'],'169.254.169.254/32','cidr'),
        ]
        for keys,value,error in cases:
            bad=copy.deepcopy(values);target=bad
            for key in keys[:-1]:target=target[key]
            target[keys[-1]]=value
            try:render(bad,error)
            except AssertionError as exc:raise AssertionError(f'Unsafe workflow setting {keys}: {exc}') from exc
        bad=copy.deepcopy(values);bad['workflow']['credentials']*=2;render(bad,'credential IDs must be unique')
        bad=copy.deepcopy(values);bad['workflow']['bindings']*=2;render(bad,'binding IDs must be unique')
        bad=copy.deepcopy(values);other=copy.deepcopy(bad['workflow']['credentials'][0]);other['id']='other';other['readTokenSecret']['name']='airflow-trigger';other['triggerTokenSecret']['name']='other-trigger';bad['workflow']['credentials'].append(other);render(bad,'distinct Secret')
        print('PASS workflow: read/trigger credential isolation, versioned DAGs, current grants, revocation and '+str(len(cases)+3)+' unsafe configurations rejected')

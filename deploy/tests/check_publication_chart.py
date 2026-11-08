"""Verify exact publisher credentials, current grant relationships and worker isolation."""
import copy
import json
from pathlib import Path
import tempfile
import yaml


def publication_values():
    subject={'issuer':'https://identity.example.com/realms/dataslate','subject':'engineer-123'}
    execution={'id':'ingest','workspaceId':'default','name':'Orders ingestion','operator':'apache',
               'image':'registry.example.com/ingestion@sha256:'+'a'*64,'namespace':'dataslate-compute',
               'serviceAccount':'engineering-driver','executorInstances':1,'executionKind':'file-ingestion-batch',
               'allowedSubjects':[subject],'ingestionStorage':{'readPrefixes':['s3://example-lake/incoming/'],
               'writePrefixes':['s3://example-lake/tables/','s3://example-lake/checkpoints/']}}
    query={'id':'read-orders','workspaceId':'default','name':'Governed orders','engine':'trino',
           'endpoint':'https://trino.example.com:8443','catalog':'lake','schema':'analytics','policyVersion':'v1',
           'maxRunSeconds':300,'readOnlyPolicyAccepted':True,'credentialsId':'reader',
           'allowedSubjects':[subject|{'engineUser':'engineer_123'}]}
    publisher={'id':'publish-orders','workspaceId':'default','name':'Orders','policyVersion':'v1',
               'pipelineId':'pipeline-orders','executionBindingId':'ingest','destinationUri':'s3://example-lake/tables/orders/',
               'endpoint':query['endpoint'],'catalog':'lake','schema':'analytics','table':'orders','maxRunSeconds':60,
               'credentialsId':'publisher','engineUser':'dataslate-publisher','registrationPolicyAccepted':True,
               'allowedPublishers':[copy.deepcopy(subject)],'readers':[subject|{'sqlBindingId':'read-orders'}]}
    return {'execution':{'enabled':True,'operatorAccepted':True,'runtimePolicyAccepted':True,
                         'namespaces':['dataslate-compute'],'kubernetesEgressCidrs':['10.0.0.1/32'],'bindings':[execution]},
            'sql':{'enabled':True,'bindings':[query],'credentials':[{'id':'reader','tokenSecret':{'name':'trino-reader','key':'token'}}],
                   'engineEgress':[{'cidr':'10.30.0.12/32','port':8443}]},
            'publication':{'enabled':True,'bindings':[publisher],
                           'credentials':[{'id':'publisher','tokenSecret':{'name':'trino-publisher','key':'token'},
                                           'caSecret':{'name':'trino-ca','key':'ca.crt'}}],
                           'engineEgress':[{'cidr':'10.30.0.12/32','port':8443}]}}


def check_publication_chart(run, external_args):
    values=publication_values()
    with tempfile.TemporaryDirectory(prefix='dataslate-chart-publication-') as directory:
        path=Path(directory)/'values.json'
        args=[*external_args,'-f',path,'--api-versions','spark.apache.org/v1/SparkApplication']
        def render(config,error=None):
            path.write_text(json.dumps(config))
            output=run(args,error)
            return {(d['kind'],d['metadata']['name']):d for d in yaml.safe_load_all(output) if d} if output is not None else {}
        docs=render(values)
        config=json.loads(docs['ConfigMap','dataslate-publication']['data']['bindings.json'])
        expected=copy.deepcopy(values['publication']['bindings'][0]);del expected['credentialsId']
        expected.update(tokenFile='/etc/dataslate/publication-credentials/publisher/token',caFile='/etc/dataslate/publication-credentials/publisher/ca.crt')
        assert config==[expected]
        deployment=docs['Deployment','dataslate-publication-controller']
        template=deployment['spec']['template'];pod=template['spec'];container=pod['containers'][0]
        assert pod['automountServiceAccountToken'] is False
        assert 'dataslate.io/tier' not in template['metadata']['labels']
        assert set(template['metadata']['annotations'])=={'checksum/execution','checksum/sql','checksum/publication'}
        assert pod['securityContext']['runAsNonRoot'] and pod['securityContext']['fsGroup']==65532
        assert container['securityContext']['readOnlyRootFilesystem'] and container['securityContext']['capabilities']['drop']==['ALL']
        assert container['ports']==[{'name':'http','containerPort':8085,'protocol':'TCP'}]
        env={v['name']:v for v in container['env']}
        assert set(env)=={'DATASLATE_PUBLICATION_CONTROLLER_ADDR','DATASLATE_PUBLICATION_BINDINGS_FILE',
                          'DATASLATE_EXECUTION_BINDINGS_FILE','DATASLATE_SQL_BINDINGS_FILE',
                          'DATASLATE_DATABASE_URL','PGSSLMODE','PGSSLROOTCERT'}
        assert env['PGSSLMODE']['value']=='verify-full'
        assert env['DATASLATE_PUBLICATION_CONTROLLER_ADDR']['value']=='0.0.0.0:8085'
        mounts={v['name']:v for v in container['volumeMounts']}
        assert mounts['publication-credential-publisher']['mountPath']=='/etc/dataslate/publication-credentials/publisher'
        assert all('subPath' not in m for m in mounts.values())
        volumes={v['name']:v for v in pod['volumes']}
        assert volumes['publication-credential-publisher']['projected']=={'defaultMode':0o440,'sources':[
            {'secret':{'name':'trino-publisher','items':[{'key':'token','path':'token'}]}},
            {'secret':{'name':'trino-ca','items':[{'key':'ca.crt','path':'ca.crt'}]}}]}
        assert volumes['execution']['configMap']['name']=='dataslate-execution'
        assert volumes['sql']['configMap']['name']=='dataslate-sql'
        for component in ('api','controller','sql-controller','web'):
            other=docs['Deployment','dataslate-'+component]['spec']['template']['spec']
            assert 'publication-credential' not in json.dumps(other)
            assert 'trino-publisher' not in json.dumps(other)
        api=docs['Deployment','dataslate-api']['spec']['template']['spec']
        assert any(e['name']=='DATASLATE_PUBLICATION_BINDINGS_FILE' for e in api['containers'][0]['env'])
        assert not any('publication-controller' in json.dumps(d) for (k,_),d in docs.items() if k in ('Role','RoleBinding','ClusterRole','ClusterRoleBinding'))
        policy=docs['NetworkPolicy','dataslate-publication-controller']['spec']
        assert policy['ingress']==[] and len(policy['egress'])==3
        assert policy['egress'][-1]=={'to':[{'ipBlock':{'cidr':'10.30.0.12/32'}}],'ports':[{'protocol':'TCP','port':8443}]}
        assert '0.0.0.0/0' not in json.dumps(policy)
        revoked=copy.deepcopy(values);revoked['publication']['bindings']=[]
        after=render(revoked)
        assert json.loads(after['ConfigMap','dataslate-publication']['data']['bindings.json'])==[]
        assert after['Deployment','dataslate-publication-controller']['spec']['template']['spec']==pod
        assert after['NetworkPolicy','dataslate-publication-controller']['spec']==policy
        revoked=copy.deepcopy(values);revoked['publication']['bindings'][0].update(allowedPublishers=[],readers=[])
        render(revoked)
        cases=[
            (['publication','enabled'],False,'publication.enabled'),
            (['publication','engineEgress'],[],'publication.engineEgress'),
            (['publication','credentials'],[],'credentialsId'),
            (['publication','credentials',0,'tokenSecret','name'],'trino-reader','separate token'),
            (['publication','credentials',0,'tokenSecret','value'],'secret','value'),
            (['publication','credentials',0,'tokenSecret','key'],'../token','key'),
            (['publication','bindings',0,'tokenFile'],'/tmp/token','tokenFile'),
            (['publication','bindings',0,'allowLocalQualification'],True,'allowLocalQualification'),
            (['publication','bindings',0,'destinationUri'],'file:///lake/orders/','destinationUri'),
            (['publication','bindings',0,'destinationUri'],'s3://example-lake/tables-other/orders/','write prefixes'),
            (['publication','bindings',0,'destinationUri'],'s3://example-lake/tables/../orders/','destinationUri'),
            (['publication','bindings',0,'table'],'orders$history','table'),
            (['publication','bindings',0,'engineUser'],'engineer_123','differ from every SQL'),
            (['publication','bindings',0,'executionBindingId'],'other','same workspace'),
            (['publication','bindings',0,'workspaceId'],'other','same workspace'),
            (['publication','bindings',0,'registrationPolicyAccepted'],False,'registrationPolicyAccepted'),
            (['publication','bindings',0,'endpoint'],'http://engine','endpoint'),
            (['publication','bindings',0,'maxRunSeconds'],0,'maxRunSeconds'),
            (['publication','bindings',0,'name'],' ','must not be blank'),
            (['publication','bindings',0,'allowedPublishers',0,'issuer'],'https://wrong.example','issuers must match'),
            (['publication','bindings',0,'allowedPublishers',0,'subject'],'other','execution grant'),
            (['publication','bindings',0,'readers',0,'subject'],'other','SQL grant'),
            (['publication','bindings',0,'readers',0,'sqlBindingId'],'other','same workspace'),
            (['sql','bindings',0,'catalog'],'other','match endpoint'),
            (['sql','bindings',0,'endpoint'],'https://other.example','match endpoint'),
            (['publication','engineEgress',0,'cidr'],'0.0.0.0/0','cidr'),
            (['publication','engineEgress',0,'cidr'],'169.254.169.254/32','cidr'),
            (['publication','engineEgress',0,'port'],65536,'port'),
        ]
        for keys,value,error in cases:
            invalid=copy.deepcopy(values);obj=invalid
            for key in keys[:-1]:obj=obj[key]
            obj[keys[-1]]=value
            try:render(invalid,error)
            except AssertionError as exc:raise AssertionError(f'Unsafe publication setting {keys}: {value!r}: {exc}') from exc
        duplicate=copy.deepcopy(values);duplicate['publication']['credentials']*=2;render(duplicate,'credential IDs must be unique')
        duplicate=copy.deepcopy(values);duplicate['publication']['bindings']*=2;render(duplicate,'binding IDs must be unique')
        duplicate=copy.deepcopy(values);second=copy.deepcopy(duplicate['publication']['bindings'][0]);second['id']='other';second['table']='ORDERS';duplicate['publication']['bindings'].append(second);render(duplicate,'targets must be unique')
        disabled=copy.deepcopy(values);disabled['sql'].update(enabled=False,bindings=[],credentials=[]);render(disabled,'requires execution.enabled and sql.enabled')
        shared=copy.deepcopy(values)
        notebook=copy.deepcopy(shared['execution']['bindings'][0])
        notebook.update(id='notebook', executionKind='notebook-python-batch')
        del notebook['ingestionStorage']
        shared['execution']['bindings'].append(notebook)
        render(shared, 'dedicated namespace')
        notebook['namespace']='dataslate-notebooks'
        shared['execution']['namespaces'].append('dataslate-notebooks')
        render(shared)
        print('PASS publication deployment: exact targets, explicit grants, credential isolation, worker-only egress, revocation config and '+str(len(cases)+5)+' unsafe configuration rejections')

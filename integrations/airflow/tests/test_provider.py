import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import ssl
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from airflow.exceptions import AirflowException
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.execution_time.task_runner import _push_xcom_if_needed
from dataslate import Client, DataSlateError
from dataslate_airflow.core import recovery_key, submit, poll, OutcomeError
from dataslate_airflow.hooks import DataSlateHook, CredentialError
from dataslate_airflow.operators import DataSlateSubmitOperator, DataSlateOperationSensor


class FakeClient:
    def __init__(self):self.calls=[];self.current={};self.outcome={}
    def accepted(self,method,args,kwargs):
        self.calls.append((method,args,kwargs));ws,binding=args[:2]
        value={'id':'run-one','workspaceId':ws,'bindingId':binding,'revision':1,'state':'queued'}
        operation=method.removeprefix('create_')
        if operation in ('notebook_run','ingestion_run','query'):
            value['ingestion' if operation=='ingestion_run' else 'notebook']={'id':args[2],'revision':args[3]}
        if operation=='query':value['notebook']['cellId']=args[4]
        if operation=='table_publication':
            value['runId']=args[1];value['bindingId']=args[2]
        key='query' if operation=='query' else 'publication' if operation=='table_publication' else 'run'
        return {key:value,'replayed':False}
    def __getattr__(self,name):
        if name.startswith('create_'):return lambda *args,**kwargs:self.accepted(name,args,kwargs)
        if name in ('query','run','table_publication'):
            def read(*args):self.calls.append((name,args,{}));return self.current
            return read
        raise AttributeError(name)
    def ingestion_outcome(self,*args):self.calls.append(('ingestion_outcome',args,{}));return self.outcome


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.context={'run_id':'scheduled__2026-11-08T06:00:00Z','ti':SimpleNamespace(dag_id='orders',task_id='ingest',map_index=-1,try_number=1)}
        self.client=FakeClient();self.request={'workspace_id':'team','binding_id':'ingest','pipeline_id':'orders','revision':4}
        self.reference=submit(self.client,'ingestion',self.request,recovery_key(self.context,'ingestion'))
        self.client.current={'id':'run-one','workspaceId':'team','bindingId':'ingest','revision':3,'state':'succeeded',
            'ingestion':{'id':'orders','revision':4},'submissionStarted':True,'kubernetesUid':'owned-uid',
            'sourceArtifact':{'state':'deleted'},'writerReleased':True}
        self.client.outcome={'runId':'run-one','workspaceId':'team','state':'verified','receipt':{
            'version':2,'runId':'run-one','pipelineId':'orders','pipelineRevision':4,'state':'succeeded','tableExists':True}}

    def test_recovery_identity_survives_retries_but_separates_runs_and_mapped_tasks(self):
        key=recovery_key(self.context,'ingestion');self.context['ti'].try_number=9
        self.assertEqual(key,recovery_key(self.context,'ingestion'))
        self.context['ti'].map_index=0;self.assertNotEqual(key,recovery_key(self.context,'ingestion'))
        self.context['ti'].map_index=-1;self.context['run_id']='manual__new';self.assertNotEqual(key,recovery_key(self.context,'ingestion'))

    def test_submission_is_one_request_with_minimal_receipt(self):
        self.assertEqual(1,len(self.client.calls))
        self.assertEqual({'version','operation','request','resourceId','minimumRevision'},set(self.reference))
        self.assertNotIn('sourceArtifact',json.dumps(self.reference))
        self.assertEqual('create_ingestion_run',self.client.calls[0][0])

    def test_only_exact_resource_fields_can_be_submitted(self):
        for bad in [self.request|{'token':'secret'},self.request|{'revision':True},self.request|{'pipeline_id':'../other'}]:
            with self.assertRaises(ValueError):submit(self.client,'ingestion',bad,'key-for-tests')
        self.assertEqual(1,len(self.client.calls))

    def test_ingestion_waits_for_writer_and_source_cleanup_and_verified_receipt(self):
        self.client.current['writerReleased']=False;self.assertFalse(poll(self.client,self.reference))
        self.client.current['writerReleased']=True;self.client.current['sourceArtifact']['state']='deleting';self.assertFalse(poll(self.client,self.reference))
        self.client.current['sourceArtifact']['state']='deleted';self.client.outcome['state']='pending';self.assertFalse(poll(self.client,self.reference))
        self.client.outcome['state']='verified';self.assertTrue(poll(self.client,self.reference))
        self.assertEqual(1,len([c for c in self.client.calls if c[0].startswith('create_')]))

    def test_success_without_owned_compute_or_valid_evidence_is_rejected(self):
        original=copy.deepcopy(self.client.current)
        for key,value in [('kubernetesUid',None),('submissionStarted',False),('sourceArtifact',None),('workspaceId','foreign')]:
            self.client.current=original|{key:value}
            with self.assertRaises(DataSlateError):poll(self.client,self.reference)
        self.client.current=original;self.client.outcome['receipt']['pipelineRevision']=5
        with self.assertRaises(DataSlateError):poll(self.client,self.reference)

    def test_failed_and_unknown_do_not_resubmit_or_infer_cancellation(self):
        self.client.current['state']='unknown';self.assertFalse(poll(self.client,self.reference))
        for state in ('failed','cancelled'):
            self.client.current['state']=state
            with self.assertRaises(OutcomeError):poll(self.client,self.reference)
        self.assertEqual(1,len([c for c in self.client.calls if c[0].startswith('create_')]))

    def test_revoked_access_remains_a_denial(self):
        with patch.object(self.client,'ingestion_outcome',side_effect=DataSlateError('forbidden','do not reveal provider detail',403)):
            with self.assertRaises(DataSlateError):poll(self.client,self.reference)

    def test_real_airflow_operator_and_rescheduling_sensor_use_same_protocol(self):
        op=DataSlateSubmitOperator(task_id='ingest',operation='ingestion',request=self.request)
        with patch.object(DataSlateHook,'get_conn',return_value=self.client):reference=op.execute(self.context)
        sensor=DataSlateOperationSensor(task_id='wait',reference=reference)
        self.assertEqual('reschedule',sensor.mode)
        with patch.object(DataSlateHook,'get_conn',return_value=self.client):result=sensor.poke(self.context)
        self.assertTrue(result.is_done);self.assertEqual(reference,result.xcom_value)
        self.client.current['state']='failed'
        with patch.object(DataSlateHook,'get_conn',return_value=self.client),self.assertRaises(AirflowFailException):sensor.poke(self.context)

    def test_real_task_runner_xcom_and_indexed_example_handoff(self):
        with DAG(dag_id='provider_xcom_transfer',schedule=None) as dag:
            sensor=DataSlateOperationSensor(task_id='await_verified_ingestion',reference=self.reference)
            downstream=DataSlateSubmitOperator(task_id='publish',operation='publication',request={
                'workspace_id':'team','binding_id':'publish','run_id':sensor.output['resourceId']})
        # Execute the real Airflow sensor success path, then its real task-runner
        # XCom expansion. Only the XCom persistence transport is replaced locally.
        runtime=SimpleNamespace(task=sensor,is_mapped=False,get_first_reschedule_date=lambda context:None)
        with patch.object(DataSlateHook,'get_conn',return_value=self.client):
            result=sensor.execute({'ti':runtime})
        stored={}
        runtime.xcom_push=lambda key,value:stored.__setitem__(key,value)
        with patch('airflow.sdk.execution_time.task_runner._xcom_push',
                   side_effect=lambda ti,key,value,**kwargs:stored.__setitem__(key,value)):
            _push_xcom_if_needed(result,runtime,Mock())
        consumer=SimpleNamespace(task=downstream,dag_id=dag.dag_id,is_mapped=False,
            xcom_pull=lambda task_ids,key,default,map_indexes:stored.get(key,default))
        self.assertEqual(self.reference,sensor.output.resolve({'ti':consumer}))
        self.assertEqual('run-one',sensor.output['resourceId'].resolve({'ti':consumer}))
        self.assertEqual({'return_value',*self.reference},set(stored))
        self.assertFalse({'token','source','result','logs','sourceArtifact'} & set(stored))
        self.assertIn(sensor.task_id,downstream.upstream_task_ids)

    def test_sensor_can_opt_out_of_indexed_xcom_keys(self):
        sensor=DataSlateOperationSensor(task_id='whole_reference_only',reference=self.reference,multiple_outputs=False)
        self.assertFalse(sensor.multiple_outputs)

    def test_task_errors_redact_upstream_body(self):
        op=DataSlateSubmitOperator(task_id='ingest',operation='ingestion',request=self.request)
        with patch.object(DataSlateHook,'get_conn',side_effect=DataSlateError('bad','sensitive-response')):
            with self.assertRaises(AirflowException) as caught:op.execute(self.context)
        self.assertNotIn('sensitive',str(caught.exception));self.assertIsNone(caught.exception.__cause__)

    def test_sensor_cannot_soft_pass_an_unverified_operation(self):
        for key in ('soft_fail','silent_fail','never_fail'):
            with self.assertRaises(ValueError):DataSlateOperationSensor(task_id='wait',reference=self.reference,**{key:True})

    def test_publication_is_verified_not_just_acknowledged(self):
        ref=submit(self.client,'publication',{'workspace_id':'team','binding_id':'publish','run_id':'run-source'},'publication-key')
        self.client.current={'id':ref['resourceId'],'workspaceId':'team','bindingId':'publish','runId':'run-source','revision':3,
            'state':'unknown','reconciliationComplete':False}
        self.assertFalse(poll(self.client,ref));self.client.current['reconciliationComplete']=True
        with self.assertRaises(OutcomeError):poll(self.client,ref)
        self.client.current.update(state='published',publishedTableId='publish',verifiedAt='2026-11-08T00:00:00Z')
        self.assertTrue(poll(self.client,ref))

    def test_all_operations_pin_saved_selections(self):
        for operation,request in [('application',{'workspace_id':'team','binding_id':'app'}),
            ('notebook',{'workspace_id':'team','binding_id':'python','notebook_id':'nb','revision':2}),
            ('sql',{'workspace_id':'team','binding_id':'sql','notebook_id':'nb','revision':2,'cell_id':'cell'})]:
            ref=submit(self.client,operation,request,'operation-key');self.assertEqual(request,ref['request'])


class TokenHandler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_POST(self):
        self.server.calls.append((self.command,self.path,self.headers.get('Authorization'),self.rfile.read(int(self.headers.get('Content-Length',0)))))
        if self.path=='/redirect':
            self.send_response(302);self.send_header('Location',self.server.origin+'/leak');self.end_headers();return
        self.send_response(200);self.end_headers();self.wfile.write(json.dumps(self.server.token_response).encode())
    def do_GET(self):
        self.server.calls.append((self.command,self.path,self.headers.get('Authorization'),None))
        self.send_response(200);self.end_headers();self.wfile.write(b'{"product":"DataSlate"}')


class ConnectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=tempfile.TemporaryDirectory(prefix='dataslate-provider-tls-');root=Path(cls.directory.name)
        key=rsa.generate_private_key(public_exponent=65537,key_size=2048);name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')]);now=datetime.now(timezone.utc)
        cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
              .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(hours=1))
              .add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True)
              .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')]),critical=False).sign(key,hashes.SHA256()))
        cls.ca=root/'ca.pem';cls.ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM));private=root/'key.pem';private.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()));private.chmod(0o600)
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),TokenHandler);context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cls.ca,private);cls.server.socket=context.wrap_socket(cls.server.socket,server_side=True)
        cls.server.origin=f'https://localhost:{cls.server.server_port}';cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join();cls.directory.cleanup()
    def setUp(self):
        self.server.calls=[];self.server.token_response={'access_token':'fixture-access-token','token_type':'Bearer','expires_in':300}
        self.conn=SimpleNamespace(host=self.server.origin,port=None,schema=None,login='fixture-client',password='fixture-secret',extra_dejson={
            'auth_mode':'oauth_client_credentials','token_endpoint':self.server.origin+'/token','ca_file':str(self.ca),'scope':'dataslate-api'})
    def hook(self):
        hook=DataSlateHook('task-service-principal')
        with patch.object(hook,'get_connection',return_value=self.conn):return hook.get_conn()
    def test_real_tls_oauth_exchange_and_sdk_request(self):
        client=self.hook();self.assertEqual({'product':'DataSlate'},client.status())
        self.assertEqual(['POST','GET'],[r[0] for r in self.server.calls]);self.assertEqual('Bearer fixture-access-token',self.server.calls[1][2])
        self.assertNotIn('fixture-secret',self.server.calls[0][3].decode());self.assertNotIn('fixture-access-token',repr(client))
    def test_untrusted_ca_and_hostname_mismatch_are_denied(self):
        for url,ca in [(self.server.origin,None),(self.server.origin.replace('localhost','127.0.0.1'),str(self.ca))]:
            with self.assertRaises(DataSlateError):Client(url,token='fixture-token',ca_file=ca,timeout=2).status()
        self.assertEqual([],self.server.calls)
    def test_redirect_never_sends_credentials_to_another_path(self):
        self.conn.extra_dejson['token_endpoint']=self.server.origin+'/redirect'
        with self.assertRaises(CredentialError):self.hook()
        self.assertEqual(['/redirect'],[c[1] for c in self.server.calls])
    def test_malformed_token_response_fails_before_api_access(self):
        for bad in [{'access_token':'fixture-access-token','token_type':'Basic','expires_in':300},
                    {'access_token':'unsafe\nvalue','token_type':'Bearer','expires_in':300},
                    {'access_token':'fixture-access-token','token_type':'Bearer','expires_in':True}]:
            self.server.token_response=bad
            with self.assertRaises(CredentialError) as caught:self.hook()
            self.assertNotIn('fixture-access-token',str(caught.exception))
        self.assertTrue(all(c[0]=='POST' for c in self.server.calls))
    def test_connection_refresh_reads_rotated_bearer_on_each_poke(self):
        self.conn.login=None;self.conn.extra_dejson={'auth_mode':'bearer','ca_file':str(self.ca)};self.conn.password='first-fixture-token';self.hook().status()
        self.conn.password='second-fixture-token';self.hook().status();self.assertEqual(['Bearer first-fixture-token','Bearer second-fixture-token'],[c[2] for c in self.server.calls])
    def test_unsafe_or_unknown_configuration_sends_no_request(self):
        originals=copy.deepcopy(self.conn.extra_dejson)
        for extra in [originals|{'verify_tls':False},originals|{'token_endpoint':'http://localhost/token'},originals|{'request_timeout':0}]:
            self.conn.extra_dejson=extra
            with self.assertRaises((CredentialError,ValueError)):self.hook()
        self.assertEqual([],self.server.calls)

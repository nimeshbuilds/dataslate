"""Shared workflow protocol over HTTP: safe recovery and observed completion."""
import contextlib
import io
import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from dataslate import Client, DataSlateError
from dataslate.cli import main
from test_publication import PublicationHandler


REVIEW = {'dag_id':'orders_daily','dag_version':3,'bundle_version':'a'*40,'policy_version':'v1'}
SELECTION = {'dagId':'orders_daily','dagVersion':3,'bundleVersion':'a'*40,'policyVersion':'v1'}

def workflow(state='queued', revision=1):
    value={'id':'workflow-one','workspaceId':'team','bindingId':'orders','policyVersion':'v1',
           'dagId':'orders_daily','dagVersion':3,'bundleVersion':'a'*40,'backendRunId':'dataslate__workflow-one',
           'revision':revision,'state':state,'dispatchStarted':state not in {'queued','checking'},
           'reconciliationComplete':state in {'succeeded','failed','unknown'}}
    if state=='succeeded':value['observedAt']='2026-11-08T00:00:00Z'
    return value


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.server=ThreadingHTTPServer(('127.0.0.1',0),PublicationHandler)
        self.server.requests,self.server.responses=[],[]
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url=f'http://127.0.0.1:{self.server.server_port}';self.client=Client(self.url)

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join()

    def test_explicit_replay_retains_exact_binding_and_key(self):
        for _ in range(2):
            with self.assertRaises(DataSlateError) as caught:self.client.create_workflow_run('team','orders',**REVIEW,idempotency_key='uncertain-request')
            self.assertEqual('connection_error',caught.exception.code)
        self.assertEqual(2,len(self.server.requests));self.assertEqual(self.server.requests[0],self.server.requests[1])
        self.assertEqual(('POST','/api/v1/workspaces/team/workflow-runs',{'bindingId':'orders','idempotencyKey':'uncertain-request',**SELECTION}),self.server.requests[0])

    def test_accepted_is_not_finished(self):
        self.server.responses=[{'run':workflow(),'replayed':False}]
        result=self.client.create_workflow_run('team','orders',**REVIEW,idempotency_key='workflow-key')
        self.assertEqual('queued',result['run']['state']);self.assertEqual(1,len(self.server.requests))

    def test_malformed_acceptance_never_confirms_success(self):
        for field,value in [('workspaceId','other'),('bindingId','other'),('id','../x'),('state',{}),('state','cancelled'),
                            ('revision',True),('dagVersion',0),('bundleVersion',None),('dispatchStarted',1),
                            ('reconciliationComplete','true')]:
            self.server.responses=[{'run':workflow()|{field:value},'replayed':False}]
            with self.assertRaises(DataSlateError) as caught:self.client.create_workflow_run('team','orders',**REVIEW,idempotency_key='workflow-key')
            self.assertEqual('invalid_response',caught.exception.code)
        self.server.responses=[{'run':workflow(),'replayed':None}]
        with self.assertRaises(DataSlateError):self.client.create_workflow_run('team','orders',**REVIEW,idempotency_key='workflow-key')

    def test_invalid_input_stays_off_network(self):
        for workspace,binding,key in [('../team','orders','workflow-key'),('team','../orders','workflow-key'),('team','orders','short')]:
            with self.assertRaises(ValueError):self.client.create_workflow_run(workspace,binding,**REVIEW,idempotency_key=key)
        for interval,wait in [(0,1),(1,float('nan')),(True,1),(-1,1)]:
            with self.assertRaises(ValueError):list(self.client.watch_workflow_run('team','workflow-one',interval=interval,max_wait=wait))
        self.assertEqual([],self.server.requests)

    def test_unknown_observation_continues_without_a_trigger(self):
        self.server.responses=[workflow('unknown',2)|{'reconciliationComplete':False},workflow('running',3),workflow('succeeded',4)]
        with patch('dataslate.client.time.sleep'):values=list(self.client.watch_workflow_run('team','workflow-one'))
        self.assertEqual(['unknown','running','succeeded'],[v['state'] for v in values])
        self.assertTrue(all(r[0]=='GET' for r in self.server.requests))

    def test_terminal_receipt_requires_dispatch_and_observation(self):
        success=workflow('succeeded',3)
        for field,value in [('dispatchStarted',False),('reconciliationComplete',False),('observedAt',None),('id','foreign')]:
            self.server.responses=[success|{field:value}]
            with self.assertRaises(DataSlateError):list(self.client.watch_workflow_run('team','workflow-one'))
        self.server.responses=[workflow('running',2)|{'reconciliationComplete':True}]
        with self.assertRaises(DataSlateError):list(self.client.watch_workflow_run('team','workflow-one'))

    def test_immutable_identity_or_revision_drift_fails_without_mutating(self):
        for field,value in [('revision',1),('backendRunId','different'),('bundleVersion','b'*40),('policyVersion','v2'),('dagVersion',4)]:
            self.server.responses=[workflow('running',2),workflow('running',3)|{field:value}]
            with patch('dataslate.client.time.sleep'),self.assertRaises(DataSlateError):list(self.client.watch_workflow_run('team','workflow-one'))
        self.assertTrue(all(r[0]=='GET' for r in self.server.requests))

    def test_timeout_does_not_cancel_or_replace_run(self):
        self.server.responses=[workflow('running',2)]
        with patch('dataslate.client.time.monotonic',side_effect=[10,20]),self.assertRaises(DataSlateError) as caught:
            list(self.client.watch_workflow_run('team','workflow-one',max_wait=1))
        self.assertEqual('watch_timeout',caught.exception.code);self.assertEqual(1,len(self.server.requests))
        self.assertEqual('GET',self.server.requests[0][0])

    def test_cli_watch_exit_codes_distinguish_final_unknown(self):
        for state,status in [('succeeded',0),('failed',3),('unknown',4)]:
            self.server.responses=[workflow(state,3)];out=io.StringIO()
            with contextlib.redirect_stdout(out):actual=main(['--url',self.url,'workflow','watch','--workspace','team','--id','workflow-one'])
            self.assertEqual(status,actual);self.assertEqual(state,json.loads(out.getvalue())['state'])
        self.assertTrue(all(r[0]=='GET' for r in self.server.requests))

    def test_cli_definitions_and_history_are_reads(self):
        for action,path in [('bindings','workflow-bindings'),('definition','workflow-bindings/orders/definition'),
                            ('history','workflow-bindings/orders/history'),('list','workflow-runs'),('get','workflow-runs/workflow-one')]:
            self.server.responses=[workflow() if action=='get' else {'items':[]}]
            args=['--url',self.url,'workflow',action,'--workspace','team']
            if action in {'definition','history'}:args+=['--binding','orders']
            if action=='get':args+=['--id','workflow-one']
            with contextlib.redirect_stdout(io.StringIO()):self.assertEqual(0,main(args))
            self.assertEqual(('GET','/api/v1/workspaces/team/'+path,None),self.server.requests[-1])

    def test_reviewed_tuple_is_required_and_never_silently_replaced(self):
        for field, value in [('policyVersion','v2'),('dagId','other_dag'),('dagVersion',4),('bundleVersion','b'*40)]:
            self.server.responses=[{'run':workflow()|{field:value},'replayed':False}]
            with self.assertRaises(DataSlateError):
                self.client.create_workflow_run('team','orders',**REVIEW,idempotency_key='workflow-key')
            self.assertEqual({'bindingId':'orders','idempotencyKey':'workflow-key',**SELECTION},self.server.requests[-1][2])
        self.assertTrue(all(request[0]=='POST' for request in self.server.requests))
        self.assertEqual(4,len(self.server.requests))

    def test_invalid_reviewed_tuple_stays_off_network(self):
        for key,value in [('dag_id','../other'),('dag_version',True),('dag_version',0),('bundle_version',''),('policy_version',None)]:
            with self.assertRaises(ValueError):
                self.client.create_workflow_run('team','orders',**(REVIEW|{key:value}),idempotency_key='workflow-key')
        with self.assertRaises(TypeError):
            self.client.create_workflow_run('team','orders',idempotency_key='workflow-key')
        self.assertEqual([],self.server.requests)

    def test_cli_trigger_sends_only_the_explicit_reviewed_tuple(self):
        self.server.responses=[{'run':workflow(),'replayed':False}]
        with contextlib.redirect_stdout(io.StringIO()):
            result=main(['--url',self.url,'workflow','trigger','--workspace','team','--binding','orders',
                         '--dag','orders_daily','--dag-version','3','--bundle-version','a'*40,
                         '--policy-version','v1','--idempotency-key','workflow-key'])
        self.assertEqual(0,result)
        self.assertEqual([('POST','/api/v1/workspaces/team/workflow-runs',
                          {'bindingId':'orders','idempotencyKey':'workflow-key',**SELECTION})],self.server.requests)

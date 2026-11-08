import contextlib
import io
import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit
from dataslate import Client
from dataslate.cli import main
from test_execution import ExecutionHandler

class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.server=ThreadingHTTPServer(('127.0.0.1',0),ExecutionHandler)
        self.server.requests,self.server.responses=[],[]
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url=f'http://127.0.0.1:{self.server.server_port}';self.client=Client(self.url)
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join()
    def test_namespace_encoding_cannot_inject_query_parameters(self):
        self.server.responses=[{'items':[],'source':'authorized-grants'}]
        namespace=['business','revenue & #1?x=y']
        self.client.catalog_tables('team','polaris',namespace)
        path=self.server.requests[0][1]
        self.assertEqual('/api/v1/workspaces/team/catalog-bindings/polaris/tables',urlsplit(path).path)
        self.assertEqual({'namespace':[json.dumps(namespace,ensure_ascii=False,separators=(',',':'))]},parse_qs(urlsplit(path).query))
    def test_table_reads_send_exact_scoped_identifiers(self):
        self.client.catalog_table('team','polaris',['finance','reports'],'orders')
        self.assertEqual(('POST','/api/v1/workspaces/team/catalog-bindings/polaris/table',{'namespace':['finance','reports'],'name':'orders'}),self.server.requests[0])
    def test_malformed_namespaces_stay_off_network(self):
        for namespace in (None,[],[''],['bad\x1fnamespace'],'not-a-list',[True]):
            with self.assertRaises(ValueError):self.client.catalog_tables('team','polaris',namespace)
        with self.assertRaises(ValueError):self.client.verify_catalog('team','../foreign')
        self.assertEqual([],self.server.requests)
    def test_cli_namespace_array_preserves_component_boundaries(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result=main(['--url',self.url,'catalog','table','--workspace','team','--binding','polaris','--namespace','["finance","quarter.one"]','--name','orders'])
        self.assertEqual(0,result)
        self.assertEqual(['finance','quarter.one'],self.server.requests[0][2]['namespace'])

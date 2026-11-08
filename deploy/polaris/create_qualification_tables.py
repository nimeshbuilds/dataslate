#!/usr/bin/env python3
"""Create only the fixed disposable FILE catalog at https://localhost:28443.

Never use with an external catalog: this is a local qualification fixture writer,
not the DataSlate adapter. Reads credentials from prepare_qualification.py output.
"""
import argparse
import json
from pathlib import Path
import ssl
import urllib.error
import urllib.parse
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

def create(directory):
    cfg=json.loads((directory/'client.json').read_text())
    if cfg['endpoint']!='https://localhost:28443/api/catalog' or cfg['realm']!='DATASLATE' or cfg['warehouse']!='qualification':raise ValueError('Only the fixed localhost qualification fixture is allowed')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(directory/'tls.crt'))),NoRedirect())
    def request(method,path,data=None,token=None,form=False):
        payload=urllib.parse.urlencode(data).encode() if form else (json.dumps(data).encode() if data is not None else None)
        req=urllib.request.Request('https://localhost:28443'+path,method=method,data=payload,headers={'Content-Type':'application/x-www-form-urlencoded' if form else 'application/json','Polaris-Realm':'DATASLATE'})
        if token:req.add_header('Authorization','Bearer '+token)
        try:
            with opener.open(req,timeout=20) as r:
                body=r.read(8*1024*1024+1)
                if len(body)>8*1024*1024:raise RuntimeError('Oversized fixture response')
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            # Never echo response bodies, secrets or tokens.
            raise RuntimeError('Fixture '+method+' '+path+' returned HTTP '+str(e.code)) from None
    token=request('POST','/api/catalog/v1/oauth/tokens',{'grant_type':'client_credentials','client_id':'qualification-root','client_secret':(directory/'client-secret').read_text(),'scope':'PRINCIPAL_ROLE:ALL'},form=True)['access_token']
    request('POST','/api/management/v1/catalogs',{'catalog':{'name':'qualification','type':'INTERNAL','properties':{'default-base-location':'file:///fixture-lake/catalog'},'storageConfigInfo':{'storageType':'FILE','allowedLocations':['file:///fixture-lake/']}}},token)
    request('PUT','/api/management/v1/principal-roles/service_admin/catalog-roles/qualification',{'catalogRole':{'name':'catalog_admin'}},token)
    for ns in [['engineering'],['engineering','raw']]:request('POST','/api/catalog/v1/qualification/namespaces',{'namespace':ns},token)
    request('POST','/api/catalog/v1/qualification/namespaces/engineering%1Fraw/tables',{'name':'orders','schema':{'type':'struct','fields':[{'id':1,'name':'order_id','required':True,'type':'long'},{'id':2,'name':'amount','required':False,'type':'decimal(18, 2)'}]},'properties':{'format-version':'2'}},token)
    denied=[]
    for path in ['/api/catalog/v1/config?warehouse=qualification','/api/catalog/v1/qualification/namespaces']:
        try:request('GET',path)
        except RuntimeError as e:
            if 'HTTP 401' not in str(e) and 'HTTP 403' not in str(e):raise
            denied.append(path)
        else:raise RuntimeError('Unauthenticated request unexpectedly succeeded')
    print(json.dumps({'createdNativeIcebergTable':True,'multipartNamespace':True,'unauthenticatedRequestsDenied':len(denied),'fixtureOnly':True}))
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--directory',required=True);a=p.parse_args();create(Path(a.directory))

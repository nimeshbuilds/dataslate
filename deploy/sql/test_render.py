import copy
import json
import os
from pathlib import Path
import unittest
from render import settings,render

ROOT=Path(__file__).resolve().parent
class ProfileTests(unittest.TestCase):
    def setUp(self): self.config=json.loads((ROOT/'profile.example.json').read_text())
    def test_invalid_network_identity_and_unknown_fields_rejected(self):
        for key,value in [('serviceIP','0.0.0.0'),('serviceIP','127.0.0.1'),('workers',True),('workers',33),('jwtAudience','x\nunsafe=true'),('tlsSecret','../secret'),('controllerLabels',{}),('egress',[{'cidr':'0.0.0.0/0','port':443}])]:
            with self.subTest(key=key,value=value):
                config=copy.deepcopy(self.config);config[key]=value
                with self.assertRaises(ValueError):settings(config)
        self.config['unknown']=True
        with self.assertRaises(ValueError):settings(self.config)
    def test_pinned_chart_renders_tls_and_isolated_credentials(self):
        helm=os.environ.get('HELM_BIN')
        chart=ROOT.parent/'.cache/trino.tgz'
        if not helm or not chart.is_file():self.skipTest('Set HELM_BIN and fetch the locked Trino chart to render')
        docs=render(self.config,helm,chart)
        service=next(d for d in docs if d and d['kind']=='Service' and d['metadata']['name']==self.config['name'])
        self.assertEqual(self.config['serviceIP'],service['spec']['clusterIP'])
        self.assertEqual(['https'],[p['name'] for p in service['spec']['ports']])
        for d in docs:
            if not d:continue
            self.assertNotIn(d['kind'],['Secret','ClusterRole','ClusterRoleBinding','CustomResourceDefinition'])
            if d['kind']=='StatefulSet':
                pod=d['spec']['template']['spec'];self.assertFalse(pod['automountServiceAccountToken'])
                container=pod['containers'][0]
                self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
                mounts={m['mountPath']:m for m in container['volumeMounts']}
                self.assertTrue(mounts['/etc/trino/auth']['readOnly'])
                self.assertTrue(mounts['/etc/trino/catalog']['readOnly'])
                self.assertEqual('4Gi',next(v for v in pod['volumes'] if v['name']=='trino-data')['emptyDir']['sizeLimit'])
            if d['kind']=='ConfigMap' and 'config.properties' in (d.get('data') or {}):
                props=d['data']['config.properties']
                self.assertEqual(1,props.count('discovery.uri='))
                self.assertIn('discovery.uri=https://dataslate-trino.dataslate-sql.svc:8443',props)
                self.assertIn('query.max-run-time=1h',props)
                self.assertIn('internal-communication.https.required=true',props)
        networks=[d for d in docs if d and d['kind']=='NetworkPolicy']
        self.assertEqual(1,len(networks));self.assertIn('Egress',networks[0]['spec']['policyTypes'])
        self.assertNotIn('0.0.0.0/0',json.dumps(networks))
    def test_image_is_pinned_and_builtin_catalogs_removed(self):
        v=settings(self.config)
        self.assertRegex(v['image']['digest'],r'^sha256:[a-f0-9]{64}$')
        self.assertEqual({'tpch':None,'tpcds':None},v['catalogs'])

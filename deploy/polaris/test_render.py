import copy
import json
import os
from pathlib import Path
import unittest
import render

class ProfileTests(unittest.TestCase):
    def setUp(self): self.config = json.loads((Path(__file__).parent / 'profile.example.json').read_text())
    def test_persistence_tls_shared_signing_and_external_ownership(self):
        v = render.settings(self.config)
        self.assertFalse(v['serviceAccount']['create'])
        self.assertEqual(v['persistence']['type'], 'relational-jdbc')
        self.assertEqual(v['advancedConfig']['quarkus.http.insecure-requests'], 'disabled')
        self.assertEqual(v['authentication']['tokenBroker']['secret']['name'], 'polaris-token-keys')
        self.assertTrue(v['image']['tag'].startswith('1.8.0@sha256:'))
        self.assertTrue(v['realmContext']['requireHeader'])
        self.assertNotIn('FILE', v['features']['SUPPORTED_CATALOG_STORAGE_TYPES'])
        self.assertNotIn('polaris.readiness.ignore-severe-issues', v['advancedConfig'])
    def test_bootstrap_has_no_credentials_or_retry_or_api_identity(self):
        policy, job = render.extras(self.config)
        p = job['spec']['template']['spec']; c = p['containers'][0]
        self.assertEqual(job['spec']['backoffLimit'], 0)
        self.assertFalse(p['automountServiceAccountToken'])
        self.assertEqual(c['args'], ['bootstrap', '--credentials-file=/bootstrap/credentials.json'])
        self.assertNotIn('--print-credentials', str(c))
        self.assertEqual(policy['spec']['policyTypes'], ['Ingress', 'Egress'])
        self.assertEqual(policy['spec']['ingress'][0]['ports'][0]['port'], 8443)
    def test_rejects_unsafe_profiles(self):
        for key, value in [('realm','x\nkey=value'),('namespace','default/other'),('databaseSchema','x;drop'),('replicas',True),('replicas',0),('clientLabels',{}),('egress',[]),('egress',[{'cidr':'0.0.0.0/0','port':443}]),('egress',[{'cidr':'169.254.169.254/32','port':80}])]:
            with self.subTest(key=key,value=value):
                c=copy.deepcopy(self.config);c[key]=value
                with self.assertRaises(ValueError): render.settings(c)
        c=copy.deepcopy(self.config);c['password']='forbidden'
        with self.assertRaises(ValueError):render.settings(c)
    def test_drift_in_upstream_chart_fails(self):
        with self.assertRaises(ValueError):render.adapt([],self.config)
        with self.assertRaises(ValueError):render.adapt([{'kind':'Secret','metadata':{'namespace':self.config['namespace']}}],self.config)

    def test_pinned_chart_has_only_scoped_resources_and_credential_references(self):
        helm = os.environ.get('HELM_BIN')
        if not helm: self.skipTest('Set HELM_BIN and fetch the locked Polaris chart to render')
        docs = render.render(self.config,helm,Path(__file__).resolve().parent.parent/'.cache/polaris.tgz')
        for doc in docs:
            self.assertEqual(self.config['namespace'],doc['metadata']['namespace'])
            self.assertNotIn(doc['kind'],('Secret','ServiceAccount','ClusterRole','ClusterRoleBinding','CustomResourceDefinition'))
            if doc['kind'] == 'Deployment':
                pod = doc['spec']['template']['spec']
                self.assertFalse(pod['automountServiceAccountToken'])
                self.assertEqual(self.config['serviceAccount'],pod['serviceAccountName'])
                self.assertTrue(pod['containers'][0]['securityContext']['readOnlyRootFilesystem'])
        job = next(doc for doc in docs if doc['kind']=='Job')
        self.assertEqual(0,job['spec']['backoffLimit'])
        self.assertNotIn('--print-credentials',str(job))

if __name__=='__main__':unittest.main()

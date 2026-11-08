"""Input boundary and rendered least-privilege structural tests; CEL is qualified on a real API server."""
import copy
import json
from pathlib import Path
import unittest

from generate import render, validate

class ComputeProfileTests(unittest.TestCase):
    def setUp(self):self.config=json.loads(Path(__file__).with_name("example.json").read_text())
    def test_rejects_unsafe_or_unknown_inputs(self):
        changes=[{"namespace":"default"},{"serviceAccount":"default"},{"apiCidrs":["fd00:ec2::254/128"]},{"s3Cidrs":["fd00:ec2::23/128"]},{"apiCidrs":["fd00:ec2::/64"]},{"namespace":"kube-system"},{"namespace":"UPPER"},{"images":["spark:latest"]},{"images":[]},{"apiCidrs":["0.0.0.0/0"]},{"apiCidrs":["169.254.169.254/32"]},{"s3Cidrs":["::/0"]},{"apiCidrs":["10.1.2.3/24"]},{"apiCidrs":[10]},{"apiPort":True},{"apiPort":0},{"maxPods":501},{"quotaCpu":0},{"unknown":"ignored?"},{"irsaRoleArn":"arn:aws:iam::123456789012:role/test"},{"stsCidrs":["10.0.0.1/32"]},{"dnsPodLabels":{}},{"serviceAccount":"admin;kubectl"}]
        for change in changes:
            with self.subTest(change=change):
                c=copy.deepcopy(self.config);c.update(change)
                with self.assertRaises((ValueError,TypeError)):render(c)
    def test_no_read_secrets_or_privilege_management(self):
        items=render(self.config)["items"]
        role=next(x for x in items if x["kind"]=="Role")
        for rule in role["rules"]:
            self.assertEqual(rule["apiGroups"],[""])
            self.assertLessEqual(set(rule["resources"]),{"pods","services","configmaps"})
            self.assertFalse(set(rule["verbs"]) & {"*","bind","escalate","impersonate"})
        self.assertFalse(any(x["kind"] in {"ClusterRole","ClusterRoleBinding","Secret"} for x in items))
    def test_namespace_is_restricted_and_network_default_deny(self):
        items=render(self.config)["items"];namespace=items[0]
        self.assertEqual(namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"],"restricted")
        self.assertEqual(namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce-version"],"v1.34")
        network=[x for x in items if x["kind"]=="NetworkPolicy"]
        deny=next(x for x in network if x["metadata"]["name"]=="dataslate-default-deny")
        self.assertEqual(deny["spec"]["podSelector"],{});self.assertEqual(deny["spec"]["ingress"],[]);self.assertEqual(deny["spec"]["egress"],[])
        self.assertFalse(any(x["metadata"]["name"] in {"dataslate-s3","dataslate-sts"} for x in network))
        rpc=next(x for x in network if x["metadata"]["name"]=="dataslate-spark-rpc")
        self.assertEqual([p["port"] for p in rpc["spec"]["egress"][0]["ports"]],[7078,7079])
    def test_admission_cannot_be_bypassed_by_changing_pod_labels(self):
        items=render(self.config)["items"]
        policy=next(x for x in items if x["kind"]=="ValidatingAdmissionPolicy")
        binding=next(x for x in items if x["kind"]=="ValidatingAdmissionPolicyBinding")
        self.assertEqual(policy["spec"]["failurePolicy"],"Fail")
        self.assertNotIn("objectSelector",policy["spec"]["matchConstraints"])
        self.assertEqual(binding["spec"]["validationActions"],["Deny"])
        self.assertEqual(binding["spec"]["matchResources"]["namespaceSelector"]["matchLabels"],{"kubernetes.io/metadata.name":self.config["namespace"]})
        expressions="\n".join(v["expression"] for v in policy["spec"]["validations"])
        for boundary in ("secretKeyRef","secretRef","automountServiceAccountToken","serviceAccountName","ephemeralContainers","hostNetwork","projected","request.userInfo.username","dataslate.io/run"):
            self.assertIn(boundary,expressions)
        self.assertNotIn("has(v.secret)",expressions);self.assertNotIn("has(v.hostPath)",expressions)
    def test_explicit_irsa_adds_only_scoped_cloud_egress(self):
        c=copy.deepcopy(self.config);c.update(irsaRoleArn="arn:aws:iam::123456789012:role/dataslate-workload",stsCidrs=["10.1.0.5/32"],s3Cidrs=["10.1.0.6/32"])
        items=render(c)["items"];sa=next(x for x in items if x["kind"]=="ServiceAccount" and x["metadata"]["name"]==c["serviceAccount"])
        self.assertEqual(sa["metadata"]["annotations"]["eks.amazonaws.com/role-arn"],c["irsaRoleArn"])
        for name in ("dataslate-s3","dataslate-sts"):
            net=next(x for x in items if x["kind"]=="NetworkPolicy" and x["metadata"]["name"]==name)
            self.assertEqual(net["spec"]["egress"][0]["ports"],[{"protocol":"TCP","port":443}])
    def test_notebook_sources_require_explicit_separate_controller_and_guard_reserved_names(self):
        c=copy.deepcopy(self.config)
        for invalid in ({}, None, {"controllerNamespace":c["namespace"],"controllerServiceAccount":"controller"}, {"controllerNamespace":"control","controllerServiceAccount":"default"}, {"controllerNamespace":"control","controllerServiceAccount":"controller","adminBypass":True}):
            c["notebookSources"]=invalid
            with self.assertRaises(ValueError):render(c)
        c["notebookSources"]={"controllerNamespace":"dataslate-control","controllerServiceAccount":"dataslate-controller"}
        items=render(c)["items"]
        policies=[x for x in items if x["kind"]=="ValidatingAdmissionPolicy"]
        self.assertEqual(len(policies),2)
        source=next(x for x in policies if x["metadata"]["name"].endswith("-sources"))
        rule=source["spec"]["matchConstraints"]["resourceRules"][0]
        self.assertEqual(set(rule["operations"]),{"CREATE","UPDATE","DELETE"});self.assertEqual(rule["resources"],["configmaps"])
        expr=source["spec"]["variables"][0]["expression"]
        self.assertIn("oldObject",expr);self.assertIn("^ds-[a-f0-9]{32}-source$",expr);self.assertNotIn("labels",expr)
        self.assertIn("system:serviceaccount:dataslate-control:dataslate-controller",source["spec"]["validations"][0]["expression"])
        role=next(x for x in items if x["kind"]=="Role")
        for rule in role["rules"]:
            if "configmaps" in rule["resources"]:self.assertNotIn("deletecollection",rule["verbs"])
    def test_render_is_deterministic_and_has_no_unbounded_volumes(self):
        self.assertEqual(render(self.config),render(self.config));items=render(self.config)["items"]
        q=next(x for x in items if x["kind"]=="ResourceQuota")["spec"]["hard"]
        self.assertEqual(q["persistentvolumeclaims"],"0");self.assertEqual(q["secrets"],"0");self.assertIn("limits.ephemeral-storage",q)

if __name__=="__main__":unittest.main()

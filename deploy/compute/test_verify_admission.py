import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from verify_admission import probes, verify
from generate import render
import copy

class AdmissionVerifierTests(unittest.TestCase):
    def setUp(self):self.config=json.loads(Path(__file__).with_name("example.json").read_text())
    def responder(self, negative_message):
        calls=[]
        def run(args,**kwargs):
            calls.append(args)
            if "validatingadmissionpolicy" in args:
                return SimpleNamespace(returncode=0,stdout=json.dumps({"metadata":{"generation":1},"status":{"observedGeneration":1,"typeChecking":{}}}),stderr="")
            if "validatingadmissionpolicybinding" in args:
                return SimpleNamespace(returncode=0,stdout=json.dumps({"spec":{"policyName":"dataslate-"+self.config["namespace"],"validationActions":["Deny"],"matchResources":{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":self.config["namespace"]}}}}}),stderr="")
            pod=json.loads(kwargs["input"])
            allowed=pod["metadata"]["name"] in {"dataslate-probe-allowed-driver","dataslate-probe-allowed-executor"}
            return SimpleNamespace(returncode=0 if allowed else 1,stdout="{}" if allowed else "",stderr="" if allowed else negative_message)
        return run,calls
    def test_denials_must_be_enforced_by_policy(self):
        for message,expected in [("Forbidden: cannot impersonate serviceaccounts",False),("connection refused",False),("violates PodSecurity restricted:v1.34",True),("ValidatingAdmissionPolicy 'test' denied request",True)]:
            with self.subTest(message=message):
                run,calls=self.responder(message)
                with patch("verify_admission.subprocess.run",side_effect=run):result=verify(self.config,"explicit-test-context")
                self.assertEqual(result["passed"],expected)
                self.assertEqual(len(result["checks"]),15)
                self.assertTrue(all("--context" in args and "explicit-test-context" in args for args in calls))
                self.assertTrue(all("--dry-run=server" in args for args in calls if "create" in args))
                self.assertFalse(any("apply" in args or "delete" in args for args in calls))
    def test_notebook_sources_cannot_report_passed_with_missing_or_altered_guard(self):
        self.config["notebookSources"]={"controllerNamespace":"control","controllerServiceAccount":"controller"}
        source_name="dataslate-"+self.config["namespace"]+"-sources"
        expected=render(self.config)["items"]
        for fault in ("missing-policy","missing-binding","wrong-controller","missing-delete","label-selector","inactive-binding","none"):
            run,calls=self.responder("ValidatingAdmissionPolicy 'test' denied request")
            def source_run(args,**kwargs):
                if source_name in args:
                    kind="ValidatingAdmissionPolicyBinding" if "validatingadmissionpolicybinding" in args else "ValidatingAdmissionPolicy"
                    if fault=="missing-policy" and kind=="ValidatingAdmissionPolicy" or fault=="missing-binding" and kind=="ValidatingAdmissionPolicyBinding":
                        return SimpleNamespace(returncode=1,stdout="",stderr="not found")
                    obj=copy.deepcopy(next(x for x in expected if x["kind"]==kind and x["metadata"]["name"]==source_name));obj["metadata"]["generation"]=1;obj["status"]={"observedGeneration":1,"typeChecking":{}}
                    if kind=="ValidatingAdmissionPolicy":
                        if fault=="wrong-controller":obj["spec"]["validations"][0]["expression"]="true"
                        if fault=="missing-delete":obj["spec"]["matchConstraints"]["resourceRules"][0]["operations"].remove("DELETE")
                        if fault=="label-selector":obj["spec"]["matchConstraints"]["objectSelector"]={"matchLabels":{"bypass":"yes"}}
                    elif fault=="inactive-binding":obj["spec"]["validationActions"]=["Warn"]
                    return SimpleNamespace(returncode=0,stdout=json.dumps(obj),stderr="")
                if "input" in kwargs and kwargs["input"] and json.loads(kwargs["input"]).get("kind")=="ConfigMap":
                    cm=json.loads(kwargs["input"]);who=args[args.index("--as")+1];allow=who.endswith(":control:controller") or cm["metadata"]["name"]=="dataslate-source-admission-ordinary"
                    return SimpleNamespace(returncode=0 if allow else 1,stdout="{}",stderr="" if allow else "ValidatingAdmissionPolicy '"+source_name+"' denied request")
                return run(args,**kwargs)
            with self.subTest(fault=fault),patch("verify_admission.subprocess.run",side_effect=source_run):
                if fault=="none":
                    result=verify(self.config,"explicit-test-context");self.assertTrue(result["passed"]);self.assertEqual(len(result["checks"]),18);self.assertIn("UPDATE/DELETE",result["scope"])
                else:
                    with self.assertRaises(ValueError):verify(self.config,"explicit-test-context")

    def test_privileged_probe_exercises_policy_not_invalid_api_combination(self):
        case=next(p for p in probes(self.config) if p[0]=="privileged")
        security=case[1]["spec"]["containers"][0]["securityContext"]
        self.assertTrue(security["privileged"]);self.assertTrue(security["allowPrivilegeEscalation"])

if __name__=="__main__":unittest.main()

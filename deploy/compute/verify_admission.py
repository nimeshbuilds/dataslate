#!/usr/bin/env python3
"""Read installed policy status and run server-side dry-run Pod admission probes. Never creates live Pods."""
from __future__ import annotations
import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys

from generate import validate, render


def probes(c):
    base={"apiVersion":"v1","kind":"Pod","metadata":{"name":"dataslate-admission-probe","namespace":c["namespace"],"labels":{"spark-role":"driver"}},"spec":{"serviceAccountName":c["serviceAccount"],"restartPolicy":"Never","securityContext":{"runAsNonRoot":True,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"spark-kubernetes-driver","image":c["images"][0],"command":["/bin/true"],"securityContext":{"allowPrivilegeEscalation":False,"capabilities":{"drop":["ALL"]}}}]}}
    operator=f'system:serviceaccount:{c["operatorNamespace"]}:{c["operatorServiceAccount"]}'
    driver=f'system:serviceaccount:{c["namespace"]}:{c["serviceAccount"]}'
    executor=copy.deepcopy(base);executor["metadata"]["labels"]["spark-role"]="executor";executor["spec"]["automountServiceAccountToken"]=False;executor["spec"]["containers"][0]["name"]="executor"
    checks=[("allowed-driver",base,operator,True),("allowed-executor",executor,driver,True)]
    def add(name,change,source=base,identity=operator):
        pod=copy.deepcopy(source);change(pod);checks.append((name,pod,identity,False))
    add("other-service-account",lambda p:p["spec"].update(serviceAccountName="default"))
    add("mutable-image",lambda p:p["spec"]["containers"][0].update(image="apache/spark:latest"))
    add("secret-volume",lambda p:p["spec"].update(volumes=[{"name":"secret","secret":{"secretName":"not-a-real-secret"}}]))
    add("secret-env",lambda p:p["spec"]["containers"][0].update(env=[{"name":"SECRET","valueFrom":{"secretKeyRef":{"name":"not-a-real-secret","key":"secret"}}}]))
    add("secret-envfrom",lambda p:p["spec"]["containers"][0].update(envFrom=[{"secretRef":{"name":"not-a-real-secret"}}]))
    add("hostpath",lambda p:p["spec"].update(volumes=[{"name":"host","hostPath":{"path":"/"}}]))
    add("unapproved-init-image",lambda p:p["spec"].update(initContainers=[{"name":"init","image":"busybox:latest","securityContext":{"allowPrivilegeEscalation":False,"capabilities":{"drop":["ALL"]}}}]))
    add("executor-api-token",lambda p:p["spec"].update(volumes=[{"name":"token","projected":{"sources":[{"serviceAccountToken":{"path":"token"}}]}}]),executor,driver)
    add("executor-automount",lambda p:p["spec"].update(automountServiceAccountToken=True),executor,driver)
    add("driver-created-by-driver",lambda p:None,base,driver)
    add("privileged",lambda p:p["spec"]["containers"][0]["securityContext"].update(privileged=True,allowPrivilegeEscalation=True))
    add("host-network",lambda p:p["spec"].update(hostNetwork=True))
    add("nonspark-role",lambda p:p["metadata"]["labels"].update({"spark-role":"other"}))
    return checks


def verify(c,context,kubectl="kubectl"):
    base=[kubectl,"--context",context]
    def command(args,body=None):
        return subprocess.run(base+args,input=None if body is None else json.dumps(body),text=True,capture_output=True,timeout=30,check=False)
    name="dataslate-"+c["namespace"]
    status=command(["get","validatingadmissionpolicy",name,"-o","json"])
    if status.returncode:raise ValueError("Cannot read the installed admission policy in the explicit context")
    policy=json.loads(status.stdout);observed=policy.get("status",{})
    if observed.get("observedGeneration") != policy["metadata"].get("generation") or observed.get("typeChecking",{}).get("expressionWarnings"):
        raise ValueError("Policy has not observed the current generation or has CEL type-check warnings")
    binding_result=command(["get","validatingadmissionpolicybinding",name,"-o","json"])
    if binding_result.returncode:raise ValueError("The matching admission binding is unavailable")
    binding=json.loads(binding_result.stdout)["spec"]
    if binding.get("policyName")!=name or binding.get("validationActions")!=["Deny"] or binding.get("matchResources",{}).get("namespaceSelector",{}).get("matchLabels")!={"kubernetes.io/metadata.name":c["namespace"]}:
        raise ValueError("The installed binding does not enforce the expected namespace policy")
    results=[]
    if "notebookSources" in c:
        # Read-only verification: inspect the exact protected-name rules and dry-run CREATE.
        # UPDATE/DELETE require a live fixture and are covered by the separate explicit local harness.
        expected_items=render(c)["items"];source_name=name+"-sources"
        expected=next(x["spec"] for x in expected_items if x["kind"]=="ValidatingAdmissionPolicy" and x["metadata"]["name"]==source_name)
        read=command(["get","validatingadmissionpolicy",source_name,"-o","json"])
        if read.returncode:raise ValueError("Notebook source admission policy is missing; notebook source protection is unverified")
        actual=json.loads(read.stdout);status=actual.get("status",{});spec=actual.get("spec",{})
        if status.get("observedGeneration")!=actual["metadata"].get("generation") or status.get("typeChecking",{}).get("expressionWarnings"):
            raise ValueError("Notebook source policy is not observed or has CEL type-check warnings")
        for field in ("failurePolicy","variables","validations"):
            if spec.get(field)!=expected[field]:raise ValueError("Notebook source policy differs from the required protected-name/controller identity rules")
        constraints=spec.get("matchConstraints",{})
        rules=copy.deepcopy(constraints.get("resourceRules",[]))
        for rule in rules:
            if rule.pop("scope","*")!="*":raise ValueError("Notebook source policy has unexpected scope")
        if rules!=expected["matchConstraints"]["resourceRules"] or constraints.get("excludeResourceRules") or constraints.get("objectSelector") or constraints.get("namespaceSelector") or spec.get("matchConditions") or spec.get("paramKind"):
            raise ValueError("Notebook source policy has exclusions or mismatched operation/resource coverage")
        read=command(["get","validatingadmissionpolicybinding",source_name,"-o","json"])
        if read.returncode:raise ValueError("Notebook source policy binding is missing")
        sb=json.loads(read.stdout).get("spec",{});match=sb.get("matchResources",{})
        if sb.get("policyName")!=source_name or sb.get("validationActions")!=["Deny"] or sb.get("paramRef") or match.get("namespaceSelector")!={"matchLabels":{"kubernetes.io/metadata.name":c["namespace"]}} or match.get("objectSelector") or match.get("resourceRules") or match.get("excludeResourceRules"):
            raise ValueError("Notebook source binding does not enforce the entire expected namespace")
        identity=c["notebookSources"];controller=f'system:serviceaccount:{identity["controllerNamespace"]}:{identity["controllerServiceAccount"]}'
        driver=f'system:serviceaccount:{c["namespace"]}:{c["serviceAccount"]}'
        source={"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"ds-00000000000000000000000000000000-source","namespace":c["namespace"]},"immutable":True,"data":{"source.py":"# admission dry-run fixture\n"}}
        for label,who,allowed,reserved in [("source-controller-create",controller,True,True),("source-driver-create-denied",driver,False,True),("source-driver-ordinary-configmap",driver,True,False)]:
            body=copy.deepcopy(source)
            if not reserved:body["metadata"]["name"]="dataslate-source-admission-ordinary"
            result=command(["create","--dry-run=server","--as",who,"-f","-","-o","json"],body)
            passed=result.returncode==0 if allowed else result.returncode!=0 and "ValidatingAdmissionPolicy" in result.stderr and source_name in result.stderr
            results.append({"name":label,"expected":"allow" if allowed else "deny","passed":passed,"diagnostic":result.stderr[:4000] if not passed else ""})
    for name,pod,identity,allow in probes(c):
        pod["metadata"]["name"]="dataslate-probe-"+name
        result=command(["create","--dry-run=server","--as",identity,"-f","-","-o","json"],pod)
        # A transport/RBAC/schema error is not evidence of the desired policy denial.
        denied_by_policy="ValidatingAdmissionPolicy" in result.stderr or "violates PodSecurity" in result.stderr
        passed=result.returncode==0 if allow else result.returncode!=0 and denied_by_policy
        results.append({"name":name,"expected":"allow" if allow else "deny","passed":passed,"diagnostic":result.stderr[:4000] if not passed else ""})
    return {"namespace":c["namespace"],"context":context,"passed":all(x["passed"] for x in results),"scope":"server-side dry-run Pod admission; notebook profiles also verify exact source policy/binding and source CREATE admission. Source UPDATE/DELETE live fixture probes, network, IAM and Spark execution are not performed","checks":results}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",required=True);parser.add_argument("--context",required=True);parser.add_argument("--kubectl",default="kubectl");args=parser.parse_args()
    try:
        config=validate(json.loads(Path(args.config).read_text()));result=verify(config,args.context,args.kubectl)
    except (OSError,ValueError,KeyError,subprocess.TimeoutExpired) as exc:parser.error(str(exc))
    print(json.dumps(result,indent=2));return 0 if result["passed"] else 1

if __name__=="__main__":sys.exit(main())

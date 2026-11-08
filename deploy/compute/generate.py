#!/usr/bin/env python3
"""Render an opt-in, single-workload-identity compute namespace; never applies resources."""
from __future__ import annotations
import argparse
import ipaddress
import json
import re
import sys
from pathlib import Path

DNS = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
IMAGE = re.compile(r"[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}\Z")
REQUIRED = {"namespace", "serviceAccount", "operatorNamespace", "operatorServiceAccount", "images", "apiCidrs", "apiPort", "dnsNamespace", "dnsPodLabels", "s3Cidrs", "stsCidrs", "irsaRoleArn", "maxPods", "quotaCpu", "quotaMemoryGi"}

def validate(c):
    if not isinstance(c, dict) or not REQUIRED.issubset(c) or set(c) - REQUIRED - {"notebookSources"}:
        raise ValueError("Configuration must contain exactly the keys shown in example.json")
    for key in ("namespace", "serviceAccount", "operatorNamespace", "operatorServiceAccount", "dnsNamespace"):
        if not isinstance(c[key], str) or not DNS.fullmatch(c[key]):
            raise ValueError(f"{key} must be a Kubernetes DNS label")
    if c["serviceAccount"] == "default":
        raise ValueError("The workload service account must be dedicated, not default")
    if c["namespace"] in {"default", c["operatorNamespace"], c["dnsNamespace"]} or c["namespace"].startswith("kube-"):
        raise ValueError("Compute requires a separate non-system namespace")
    if not isinstance(c["images"], list) or not 1 <= len(c["images"]) <= 16 or len(set(c["images"])) != len(c["images"]) or any(not isinstance(i, str) or not IMAGE.fullmatch(i) for i in c["images"]):
        raise ValueError("images must contain 1–16 unique SHA-256-pinned OCI references")
    for field in ("apiCidrs", "s3Cidrs", "stsCidrs"):
        if not isinstance(c[field], list) or len(c[field]) > 100 or (field == "apiCidrs" and not c[field]):
            raise ValueError(f"{field} must be an explicit bounded CIDR array")
        for value in c[field]:
            if not isinstance(value, str): raise ValueError(f"{field} must contain CIDR strings")
            network = ipaddress.ip_network(value, strict=True)
            if network.prefixlen == 0 or network.is_multicast or network.is_loopback or network.is_link_local:
                raise ValueError(f"{field} must not permit all addresses, metadata/link-local, multicast or loopback")
            for denied in (ipaddress.ip_network("169.254.0.0/16"), ipaddress.ip_network("fe80::/10"), ipaddress.ip_network("fd00:ec2::254/128"), ipaddress.ip_network("fd00:ec2::23/128")):
                if network.version == denied.version and network.overlaps(denied):
                    raise ValueError(f"{field} must not overlap link-local or AWS metadata/Pod Identity addresses")
    for key, low, high in (("apiPort", 1, 65535), ("maxPods", 2, 500), ("quotaCpu", 1, 1000), ("quotaMemoryGi", 2, 4096)):
        if type(c[key]) is not int or not low <= c[key] <= high:
            raise ValueError(f"{key} must be an integer in {low}..{high}")
    if not isinstance(c["dnsPodLabels"], dict) or not c["dnsPodLabels"] or len(c["dnsPodLabels"]) > 8 or any(not isinstance(k, str) or not isinstance(v, str) or len(k)>253 or len(v)>63 or not k or not v for k,v in c["dnsPodLabels"].items()):
        raise ValueError("dnsPodLabels must select the actual cluster DNS Pods")
    if not isinstance(c["irsaRoleArn"], str) or (c["irsaRoleArn"] and not re.fullmatch(r"arn:aws(?:-us-gov|-cn)?:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", c["irsaRoleArn"])):
        raise ValueError("irsaRoleArn must be empty or an explicit AWS IAM role ARN")
    if bool(c["irsaRoleArn"]) != bool(c["stsCidrs"]):
        raise ValueError("IRSA requires explicit STS egress CIDRs; STS egress without IRSA is rejected")
    if "notebookSources" in c:
        source = c["notebookSources"]
        if not isinstance(source, dict) or set(source) != {"controllerNamespace", "controllerServiceAccount"}:
            raise ValueError("notebookSources requires explicit controllerNamespace and controllerServiceAccount")
        if any(not isinstance(v,str) or not DNS.fullmatch(v) for v in source.values()) or source["controllerServiceAccount"] == "default" or source["controllerNamespace"] == c["namespace"]:
            raise ValueError("Source controller requires a dedicated identity outside the compute namespace")
    return c

def resource(kind, name, namespace=None, api="v1", **fields):
    metadata = {"name":name, "labels":{"app.kubernetes.io/managed-by":"dataslate-compute-profile"}}
    if namespace: metadata["namespace"] = namespace
    return {"apiVersion":api,"kind":kind,"metadata":metadata,**fields}

def render(config):
    c=validate(config);ns=c["namespace"];sa=c["serviceAccount"];name="dataslate-"+ns
    nsobj=resource("Namespace",ns)
    nsobj["metadata"]["labels"].update({"pod-security.kubernetes.io/enforce":"restricted","pod-security.kubernetes.io/enforce-version":"v1.34","pod-security.kubernetes.io/audit":"restricted","pod-security.kubernetes.io/audit-version":"v1.34","pod-security.kubernetes.io/warn":"restricted","pod-security.kubernetes.io/warn-version":"v1.34"})
    account=resource("ServiceAccount",sa,ns,automountServiceAccountToken=True)
    if c["irsaRoleArn"]: account["metadata"]["annotations"]={"eks.amazonaws.com/role-arn":c["irsaRoleArn"],"eks.amazonaws.com/sts-regional-endpoints":"true"}
    default=resource("ServiceAccount","default",ns,automountServiceAccountToken=False)
    role=resource("Role","dataslate-spark-driver",ns,api="rbac.authorization.k8s.io/v1",rules=[{"apiGroups":[""],"resources":["pods"],"verbs":["get","list","watch","create","patch","delete","deletecollection"]},{"apiGroups":[""],"resources":["services","configmaps"],"verbs":["get","list","watch","create","delete","deletecollection"]}])
    binding=resource("RoleBinding","dataslate-spark-driver",ns,api="rbac.authorization.k8s.io/v1",roleRef={"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"dataslate-spark-driver"},subjects=[{"kind":"ServiceAccount","name":sa,"namespace":ns}])
    quota=resource("ResourceQuota","dataslate-compute",ns,spec={"hard":{"pods":str(c["maxPods"]),"requests.cpu":str(c["quotaCpu"]),"limits.cpu":str(c["quotaCpu"]),"requests.memory":str(c["quotaMemoryGi"])+"Gi","limits.memory":str(c["quotaMemoryGi"])+"Gi","requests.ephemeral-storage":"100Gi","limits.ephemeral-storage":"200Gi","persistentvolumeclaims":"0","secrets":"0","services.loadbalancers":"0","services.nodeports":"0","count/configmaps":"200","count/services":"100"}})
    limits=resource("LimitRange","dataslate-compute",ns,spec={"limits":[{"type":"Container","defaultRequest":{"cpu":"500m","memory":"1Gi","ephemeral-storage":"1Gi"},"default":{"cpu":"1","memory":"2Gi","ephemeral-storage":"2Gi"},"max":{"cpu":"4","memory":"8Gi","ephemeral-storage":"10Gi"},"min":{"cpu":"100m","memory":"128Mi","ephemeral-storage":"128Mi"}}]})
    def net(name,spec):return resource("NetworkPolicy",name,ns,api="networking.k8s.io/v1",spec=spec)
    defaultdeny=net("dataslate-default-deny",{"podSelector":{},"policyTypes":["Ingress","Egress"],"ingress":[],"egress":[]})
    dns=net("dataslate-dns",{"podSelector":{},"policyTypes":["Egress"],"egress":[{"to":[{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":c["dnsNamespace"]}},"podSelector":{"matchLabels":c["dnsPodLabels"]}}],"ports":[{"protocol":"UDP","port":53},{"protocol":"TCP","port":53}]}]})
    peer={"podSelector":{"matchExpressions":[{"key":"spark-role","operator":"In","values":["driver","executor"]}]}}
    ports=[{"protocol":"TCP","port":7078},{"protocol":"TCP","port":7079}]
    spark=net("dataslate-spark-rpc",{"podSelector":{"matchExpressions":[{"key":"spark-role","operator":"In","values":["driver","executor"]}]},"policyTypes":["Ingress","Egress"],"ingress":[{"from":[peer],"ports":ports}],"egress":[{"to":[peer],"ports":ports}]})
    api=net("dataslate-driver-api",{"podSelector":{"matchLabels":{"spark-role":"driver"}},"policyTypes":["Egress"],"egress":[{"to":[{"ipBlock":{"cidr":cidr}} for cidr in c["apiCidrs"]],"ports":[{"protocol":"TCP","port":c["apiPort"]}]}]})
    resources=[nsobj,account,default,role,binding,quota,limits,defaultdeny,dns,spark,api]
    for target in ("s3","sts"):
        if c[target+"Cidrs"]:resources.append(net("dataslate-"+target,{"podSelector":{},"policyTypes":["Egress"],"egress":[{"to":[{"ipBlock":{"cidr":cidr}} for cidr in c[target+"Cidrs"]],"ports":[{"protocol":"TCP","port":443}]}]}))
    # All matching Pods must satisfy this policy, independent of caller-controlled labels.
    role_expr="has(object.metadata.labels) && 'spark-role' in object.metadata.labels ? object.metadata.labels['spark-role'] : ''"
    container_expr="object.spec.containers + (has(object.spec.initContainers) ? object.spec.initContainers : [])"
    operator="system:serviceaccount:"+c["operatorNamespace"]+":"+c["operatorServiceAccount"]
    driver="system:serviceaccount:"+ns+":"+sa
    audience=json.dumps(["sts.amazonaws.com"] if c["irsaRoleArn"] else [])
    token="has(s.serviceAccountToken) && ((variables.role == 'driver' && (!has(s.serviceAccountToken.audience) || s.serviceAccountToken.audience in "+audience+")) || (variables.role == 'executor' && has(s.serviceAccountToken.audience) && s.serviceAccountToken.audience in "+audience+"))"
    validations=[
      ("variables.role in ['driver','executor']","Only Spark driver/executor Pods are admitted."),
      ("request.operation != 'UPDATE' || (has(oldObject.metadata.labels) && 'spark-role' in oldObject.metadata.labels && oldObject.metadata.labels['spark-role'] == variables.role)","A Pod's Spark role cannot be changed after creation."),
      ("has(object.spec.serviceAccountName) && object.spec.serviceAccountName == "+json.dumps(sa),"The approved workload service account is required."),
      ("request.operation != 'CREATE' || request.userInfo.username == "+json.dumps(operator)+" || (request.userInfo.username == "+json.dumps(driver)+" && variables.role == 'executor')","Only the operator may create drivers; the approved driver may create executors."),
      ("variables.containers.all(c, c.image in "+json.dumps(c["images"])+")","All containers must use a configured immutable image digest."),
      ("!has(object.spec.imagePullSecrets) || size(object.spec.imagePullSecrets) == 0","Pod-supplied registry Secrets are unsupported; use node registry identity."),
      ("!has(object.spec.ephemeralContainers) || size(object.spec.ephemeralContainers) == 0","Ephemeral debug containers are not enabled in this profile."),
      ("variables.role != 'executor' || (has(object.spec.automountServiceAccountToken) && !object.spec.automountServiceAccountToken)","Executors must disable the default Kubernetes API token mount."),
      ("variables.containers.all(c, (!has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef))) && (!has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef))))","Secret environment references are forbidden."),
      ("!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.emptyDir) || has(v.configMap) || has(v.downwardAPI) || (has(v.projected) && v.projected.sources.all(s, has(s.configMap) || has(s.downwardAPI) || ("+token+"))))","Only emptyDir, ConfigMap, downward API and narrowly scoped projected identity volumes are allowed."),
      ("!has(object.spec.hostNetwork) || !object.spec.hostNetwork","Host networking is forbidden."),
      ("!has(object.spec.hostPID) || !object.spec.hostPID","Host PID sharing is forbidden."),
      ("!has(object.spec.hostIPC) || !object.spec.hostIPC","Host IPC sharing is forbidden."),
    ]
    policy=resource("ValidatingAdmissionPolicy",name,api="admissionregistration.k8s.io/v1",spec={"failurePolicy":"Fail","matchConstraints":{"resourceRules":[{"apiGroups":[""],"apiVersions":["v1"],"operations":["CREATE","UPDATE"],"resources":["pods","pods/ephemeralcontainers"]}]},"variables":[{"name":"role","expression":role_expr},{"name":"containers","expression":container_expr}],"validations":[{"expression":expr,"message":message,"reason":"Forbidden"} for expr,message in validations]})
    admissionbinding=resource("ValidatingAdmissionPolicyBinding",name,api="admissionregistration.k8s.io/v1",spec={"policyName":name,"validationActions":["Deny"],"matchResources":{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":ns}}}})
    resources += [policy,admissionbinding]
    if "notebookSources" in c:
        # Collection deletion bypasses individual resource-name constraints; never grant it for source namespaces.
        role["rules"][1]["resources"] = ["services"]
        role["rules"].append({"apiGroups":[""],"resources":["configmaps"],"verbs":["get","list","watch","create","delete"]})
        controller = "system:serviceaccount:"+c["notebookSources"]["controllerNamespace"]+":"+c["notebookSources"]["controllerServiceAccount"]
        source_name = name+"-sources"
        source_policy = resource("ValidatingAdmissionPolicy",source_name,api="admissionregistration.k8s.io/v1",spec={
          "failurePolicy":"Fail",
          "matchConstraints":{"resourceRules":[{"apiGroups":[""],"apiVersions":["v1"],"operations":["CREATE","UPDATE","DELETE"],"resources":["configmaps"]}]},
          "variables":[{"name":"reserved","expression":"(object != null && object.metadata.name.matches('^ds-[a-f0-9]{32}-source$')) || (oldObject != null && oldObject.metadata.name.matches('^ds-[a-f0-9]{32}-source$'))"}],
          "validations":[{"expression":"!variables.reserved || request.userInfo.username == "+json.dumps(controller),"message":"Only the configured DataSlate controller may manage reserved notebook source ConfigMaps.","reason":"Forbidden"}]})
        source_binding = resource("ValidatingAdmissionPolicyBinding",source_name,api="admissionregistration.k8s.io/v1",spec={"policyName":source_name,"validationActions":["Deny"],"matchResources":{"namespaceSelector":{"matchLabels":{"kubernetes.io/metadata.name":ns}}}})
        resources += [source_policy,source_binding]
    return {"apiVersion":"v1","kind":"List","items":resources}

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",required=True);parser.add_argument("--output",help="Destination JSON manifest; stdout if omitted")
    args=parser.parse_args(argv)
    try:
        raw=Path(args.config).read_bytes()
        if len(raw)>128*1024:raise ValueError("Profile input exceeds 128 KiB")
        output=json.dumps(render(json.loads(raw)),indent=2)+"\n"
        if args.output:
            # Refuse overwriting a reviewed manifest accidentally.
            with open(args.output,"x") as stream:stream.write(output)
        else:sys.stdout.write(output)
    except (OSError,ValueError,TypeError) as exc:parser.error(str(exc))

if __name__=="__main__":main()

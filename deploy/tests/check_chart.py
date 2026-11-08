#!/usr/bin/env python3
"""Exercise actual Helm renders and deployment security invariants, without a cluster."""
import argparse
from check_workflow_chart import check_workflow_chart
import ipaddress
import os
from pathlib import Path
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm/dataslate"
EXTERNAL = ROOT / "examples/eks-external.yaml"
MANAGED = ROOT / "examples/eks-managed-postgres.yaml"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--helm", default="helm")
    args = parser.parse_args()
    # Prevent accidental credential discovery. Commands are lint/template only.
    env = dict(os.environ, KUBECONFIG=os.devnull)

    def run(argv, expected_error=None):
        result = subprocess.run([args.helm, *map(str, argv)], text=True, capture_output=True, env=env)
        if expected_error:
            assert result.returncode != 0, "Unsafe configuration unexpectedly rendered: " + str(argv)
            assert expected_error in result.stdout + result.stderr, result.stdout + result.stderr
            return None
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout

    run(["lint", CHART, "-f", EXTERNAL, "--strict", "--kube-version", "1.34.0"])
    template = ["template", "dataslate", CHART, "--namespace", "dataslate", "--kube-version", "1.34.0"]
    external_args = [*template, "-f", EXTERNAL]
    managed_args = [*external_args, "-f", MANAGED, "--api-versions", "postgresql.cnpg.io/v1/Cluster"]
    examples = {"external": run(external_args), "managed": run(managed_args)}

    for name, rendered in examples.items():
        docs = [doc for doc in yaml.safe_load_all(rendered) if doc]
        objects = {(d["kind"], d["metadata"]["name"]): d for d in docs}
        assert len(objects) == len(docs), "Duplicate resource identities"
        assert not {"Secret", "Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition"}.intersection(d["kind"] for d in docs)
        for service in [d for d in docs if d["kind"] == "Service"]:
            assert service["spec"]["type"] == "ClusterIP"
            assert service["spec"]["ports"][0]["port"] == 8080
        for doc in [d for d in docs if d["kind"] in ["Deployment", "Job"]]:
            pod = doc["spec"]["template"]["spec"]
            assert pod["automountServiceAccountToken"] is False
            assert pod["securityContext"]["runAsNonRoot"] is True
            assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
            assert not pod.get("hostNetwork") and not pod.get("hostPID")
            for c in pod["containers"]:
                sec = c["securityContext"]
                assert sec["allowPrivilegeEscalation"] is False
                assert sec["readOnlyRootFilesystem"] is True
                assert sec["runAsNonRoot"] is True
                assert sec["capabilities"]["drop"] == ["ALL"]
                assert all(c["resources"][kind][r] for kind in ["requests", "limits"] for r in ["cpu", "memory"])
                if doc["kind"] == "Deployment":
                    assert all(c[probe]["httpGet"]["port"] == "http" for probe in ["startupProbe", "readinessProbe", "livenessProbe"])
        api = objects[("Deployment", "dataslate-api")]["spec"]["template"]["spec"]
        api_env = {v["name"]: v for v in api["containers"][0]["env"]}
        assert api_env["DATASLATE_AUTH_MODE"]["value"] == "oidc"
        assert api_env["PGSSLMODE"]["value"] == "verify-full"
        assert api_env["PGSSLROOTCERT"]["value"] == "/etc/dataslate/postgres/ca.crt"
        assert "value" not in api_env["DATASLATE_DATABASE_URL"]
        secret = api_env["DATASLATE_DATABASE_URL"]["valueFrom"]["secretKeyRef"]
        assert secret == ({"name": "dataslate-db-app", "key": "uri"} if name == "managed" else {"name": "dataslate-database", "key": "database-url"})
        web = objects[("Deployment", "dataslate-web")]["spec"]["template"]["spec"]["containers"][0]
        assert web["env"] == [{"name": "DATASLATE_API_UPSTREAM", "value": "http://dataslate-api:8080"}]
        assert web["readinessProbe"]["httpGet"]["path"] == "/web-healthz"
        jobs = [d for d in docs if d["kind"] == "Job"]
        assert len(jobs) == 1 and jobs[0]["spec"]["template"]["spec"]["containers"][0]["args"] == ["migrate"]
        assert "helm.sh/hook" not in jobs[0]["metadata"].get("annotations", {}), "pre-install migration would deadlock managed DB bootstrap"
        policies = [d for d in docs if d["kind"] == "NetworkPolicy"]
        assert len(policies) == 2
        for policy in policies:
            assert set(policy["spec"]["policyTypes"]) == {"Ingress", "Egress"}
            for rule in policy["spec"].get("egress", []):
                assert rule.get("ports") and rule.get("to"), "Unbounded egress rule"
                for peer in rule["to"]:
                    if "ipBlock" in peer:
                        ipaddress.ip_network(peer["ipBlock"]["cidr"], strict=False)
        if name == "managed":
            db = objects[("Cluster", "dataslate-db")]
            assert db["apiVersion"] == "postgresql.cnpg.io/v1"
            assert db["spec"]["instances"] == 3
            assert db["spec"]["storage"] == {"storageClass": "dataslate-gp3", "size": "50Gi"}
            assert db["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
            assert db["spec"]["enableSuperuserAccess"] is False
        else:
            assert not any(d["kind"] == "Cluster" for d in docs)
        print(f"PASS {name}: {len(docs)} resources; identity, TLS, persistence, probes, network and privilege boundaries")

    negatives = [
        (template, "oidc.issuer is required"),
        ([*external_args, "--set", "oidc.issuer=http://insecure.example.com"], "insecure.example.com"),
        ([*external_args, "--set", "auth.mode=development"], "auth"),
        ([*external_args, "--set", "oidc.audience=dataslate-web"], "browser ID tokens"),
        ([*external_args, "--set", "postgres.mode=memory"], "external"),
        ([*external_args, "--set", "postgres.external.caSecretName="], "trusted CA secret"),
        ([*external_args, "--set", "ingress.tls.externallyTerminated=false"], "TLS secret"),
        ([*external_args, "--set", "api.replicas=1"], "PDB minAvailable"),
        ([*managed_args, "--set", "postgres.managed.operatorAccepted=false"], "operatorAccepted=true"),
        ([*external_args, "-f", MANAGED], "Cluster CRD is required"),
        ([*managed_args, "--set", "postgres.managed.storageClass="], "persistent storage class"),
        ([*external_args, "--set", "api.image.digest=sha256:wrong"], "sha256:wrong"),
        ([*external_args, "--set-json", "networkPolicy.httpsEgressPorts=[]"], "httpsEgressPorts"),
        ([*external_args, "--set-json", "networkPolicy.httpsEgressPorts=[0]"], "httpsEgressPorts"),
        ([*external_args, "--set-json", "networkPolicy.httpsEgressPorts=[65536]"], "httpsEgressPorts"),
        ([*external_args, "--set", "networkPolicy.postgresPort=0"], "postgresPort"),
        ([*external_args, "--set", "networkPolicy.postgresPort=65536"], "postgresPort"),
        ([*managed_args, "--set", "networkPolicy.postgresPort=6543"], "external PostgreSQL only"),
    ]
    for argv, error in negatives:
        run(argv, error)
    print(f"PASS {len(negatives)} invalid/unsafe configuration rejection cases")

    custom_ports = [d for d in yaml.safe_load_all(run([
        *external_args,
        "--set", "oidc.issuer=https://identity.example.com:8443/realms/dataslate",
        "--set-json", "networkPolicy.httpsEgressPorts=[8443,9443]",
        "--set", "networkPolicy.postgresPort=6543",
    ])) if d]
    policy = next(d for d in custom_ports if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "dataslate-backend")
    assert policy["spec"]["egress"][1]["ports"] == [{"protocol": "TCP", "port": 8443}, {"protocol": "TCP", "port": 9443}]
    assert policy["spec"]["egress"][2]["ports"] == [{"protocol": "TCP", "port": 6543}]
    api = next(d for d in custom_ports if d["kind"] == "Deployment" and d["metadata"]["name"] == "dataslate-api")
    issuer = next(e for e in api["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "DATASLATE_OIDC_ISSUER")
    assert issuer["value"] == "https://identity.example.com:8443/realms/dataslate"
    print("PASS custom HTTPS and PostgreSQL ports: rendered policy matches explicit endpoint configuration")

    from check_execution_chart import check_execution_chart
    check_execution_chart(run, external_args)
    from check_sql_chart import check_sql_chart
    check_sql_chart(run, external_args)
    from check_publication_chart import check_publication_chart
    check_publication_chart(run, external_args)
    check_workflow_chart(run, external_args)
    from check_catalog_chart import check_catalog_chart
    check_catalog_chart(run, external_args)
    from check_search_chart import check_search_chart
    check_search_chart(run, external_args, managed_args)
    from check_sylon_chart import check_sylon_chart
    check_sylon_chart(run, external_args, managed_args)

    # Native ClusterIP/no ingress profile must be usable without exposing the workspace.
    docs = list(yaml.safe_load_all(run([*external_args, "--set", "ingress.enabled=false"])))
    assert not any(d and d["kind"] == "Ingress" for d in docs)
    # Operator charts are optional cache inputs, never fetched or installed by tests.
    for component, values in [("spark-operator", "spark-operator-values.yaml"), ("cloudnative-pg", "cloudnative-pg-values.yaml")]:
        package = ROOT / ".cache" / (component + ".tgz")
        if package.exists():
            run(["lint", package, "-f", ROOT / "bootstrap" / values])
            upstream = run(["template", "dataslate-" + component, package, "-f", ROOT / "bootstrap" / values, "--namespace", "dataslate-operators", "--kube-version", "1.34.0"])
            parsed = [d for d in yaml.safe_load_all(upstream) if d]
            if component == "spark-operator":
                assert not any(d["kind"] == "Namespace" for d in parsed)
                assert not any(d["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "spark-workload" for d in parsed)
                assert any("spark.kubernetes.operator.watchedNamespaces=dataslate-compute" in str(d.get("data", {})) for d in parsed)
            print("PASS upstream optional bootstrap render: " + component)
    print("No cluster accessed; live EKS, database, image and OIDC tests remain required.")


if __name__ == "__main__":
    main()

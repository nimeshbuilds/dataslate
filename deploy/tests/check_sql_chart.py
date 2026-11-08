"""Render SQL controller wiring and verify credential, identity and network scope."""
import copy
import ipaddress
import json
from pathlib import Path
import tempfile

import yaml


def check_sql_chart(run, external_args):
    binding = {
        "id": "analytics", "workspaceId": "default", "name": "Governed analytics",
        "engine": "trino", "endpoint": "https://trino.example.com:8443",
        "catalog": "lakehouse", "schema": "analytics", "policyVersion": "v1",
        "maxRunSeconds": 300, "readOnlyPolicyAccepted": True, "credentialsId": "analytics",
        "allowedSubjects": [{"issuer": "https://identity.example.com/realms/dataslate",
                             "subject": "engineer-123", "engineUser": "engineer_123"}],
    }
    values = {"sql": {"enabled": True, "bindings": [binding], "credentials": [{
        "id": "analytics", "tokenSecret": {"name": "trino-client", "key": "access-token"},
        "caSecret": {"name": "trino-ca", "key": "trust.pem"},
    }], "engineEgress": [{"cidr": "10.30.0.12/32", "port": 8443}]}}
    with tempfile.TemporaryDirectory(prefix="dataslate-chart-sql-") as directory:
        path = Path(directory) / "sql.json"
        args = [*external_args, "-f", path]

        def render(config, expected_error=None, extra=()):
            path.write_text(json.dumps(config))
            output = run([*args, *extra], expected_error)
            return [d for d in yaml.safe_load_all(output) if d] if output is not None else []

        docs = render(values)
        objects = {(d["kind"], d["metadata"]["name"]): d for d in docs}
        assert len(objects) == len(docs)
        assert not {"Secret", "Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition"}.intersection(d["kind"] for d in docs)
        assert len([d for d in docs if d["kind"] == "Deployment"]) == 3
        assert objects[("ServiceAccount", "dataslate-sql-controller")]["automountServiceAccountToken"] is False
        config = objects[("ConfigMap", "dataslate-sql")]
        actual = json.loads(config["data"]["bindings.json"])
        expected = copy.deepcopy(binding)
        del expected["credentialsId"]
        expected.update(tokenFile="/etc/dataslate/sql-credentials/analytics/token",
                        caFile="/etc/dataslate/sql-credentials/analytics/ca.crt")
        assert actual == [expected]
        assert "trino-client" not in json.dumps(config) and "access-token" not in json.dumps(config)

        api = objects[("Deployment", "dataslate-api")]["spec"]["template"]["spec"]
        assert api["automountServiceAccountToken"] is False
        assert not any(v.get("projected") for v in api["volumes"])
        assert not any("sql-credential" in json.dumps(v) for v in api["volumes"])
        api_env = {v["name"]: v for v in api["containers"][0]["env"]}
        assert api_env["DATASLATE_SQL_BINDINGS_FILE"]["value"] == "/etc/dataslate/sql/bindings.json"
        assert "DATASLATE_SQL_CONTROLLER_ADDR" not in api_env
        assert any(v.get("configMap", {}).get("name") == "dataslate-sql" for v in api["volumes"])

        template = objects[("Deployment", "dataslate-sql-controller")]["spec"]["template"]
        assert "dataslate.io/tier" not in template["metadata"]["labels"], "SQL must not inherit the API OIDC egress policy"
        pod = template["spec"]
        assert pod["serviceAccountName"] == "dataslate-sql-controller"
        assert pod["automountServiceAccountToken"] is False
        assert pod["securityContext"] == {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"},
                                          "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532}
        container = pod["containers"][0]
        assert container["securityContext"] == {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                               "runAsNonRoot": True, "capabilities": {"drop": ["ALL"]}}
        assert container["ports"] == [{"name": "http", "containerPort": 8082, "protocol": "TCP"}]
        env = {v["name"]: v for v in container["env"]}
        assert set(env) == {"DATASLATE_SQL_CONTROLLER_ADDR", "DATASLATE_SQL_BINDINGS_FILE",
                            "DATASLATE_DATABASE_URL", "PGSSLMODE", "PGSSLROOTCERT"}
        assert env["PGSSLMODE"]["value"] == "verify-full"
        assert env["PGSSLROOTCERT"]["value"] == "/etc/dataslate/postgres/ca.crt"
        assert env["DATASLATE_DATABASE_URL"]["valueFrom"]["secretKeyRef"] == {"name": "dataslate-database", "key": "database-url"}
        assert env["DATASLATE_SQL_CONTROLLER_ADDR"]["value"] == "0.0.0.0:8082"
        assert all(container[p]["httpGet"]["port"] == "http" for p in ("startupProbe", "readinessProbe", "livenessProbe"))
        assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
        mounts = {v["name"]: v for v in container["volumeMounts"]}
        assert mounts["sql-credential-analytics"] == {"name": "sql-credential-analytics", "mountPath": "/etc/dataslate/sql-credentials/analytics", "readOnly": True}
        assert all("subPath" not in v for v in container["volumeMounts"]), "Projected credentials must be rotatable"
        volumes = {v["name"]: v for v in pod["volumes"]}
        assert volumes["sql-credential-analytics"]["projected"] == {
            "defaultMode": 0o440,
            "sources": [{"secret": {"name": "trino-client", "items": [{"key": "access-token", "path": "token"}]}},
                        {"secret": {"name": "trino-ca", "items": [{"key": "trust.pem", "path": "ca.crt"}]}}],
        }
        assert volumes["tmp"]["emptyDir"]["sizeLimit"] == "64Mi"
        policy = objects[("NetworkPolicy", "dataslate-sql-controller")]["spec"]
        assert set(policy["policyTypes"]) == {"Ingress", "Egress"} and policy["ingress"] == []
        assert len(policy["egress"]) == 3
        dns, database, engine = policy["egress"]
        assert dns["to"][0]["namespaceSelector"]["matchLabels"] == {"kubernetes.io/metadata.name": "kube-system"}
        assert dns["to"][0]["podSelector"]["matchLabels"] == {"k8s-app": "kube-dns"}
        assert database == {"to": [{"ipBlock": {"cidr": "10.20.0.0/24"}}, {"ipBlock": {"cidr": "10.20.1.0/24"}}], "ports": [{"protocol": "TCP", "port": 5432}]}
        assert engine == {"to": [{"ipBlock": {"cidr": "10.30.0.12/32"}}], "ports": [{"protocol": "TCP", "port": 8443}]}
        assert "0.0.0.0/0" not in json.dumps(policy)
        assert not any(d["kind"] == "Service" and "sql-controller" in d["metadata"]["name"] for d in docs)

        # Revocation must keep old credential paths and network destinations available for cancellation.
        drained = copy.deepcopy(values)
        drained["sql"]["bindings"] = []
        d = {(d["kind"], d["metadata"]["name"]): d for d in render(drained)}
        assert json.loads(d[("ConfigMap", "dataslate-sql")]["data"]["bindings.json"]) == []
        assert d[("Deployment", "dataslate-sql-controller")]["spec"]["template"]["spec"] == pod
        assert d[("NetworkPolicy", "dataslate-sql-controller")]["spec"] == policy
        revoked = copy.deepcopy(values)
        revoked["sql"]["bindings"][0]["allowedSubjects"] = []
        assert json.loads(next(d for d in render(revoked) if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "dataslate-sql")["data"]["bindings.json"])[0]["allowedSubjects"] == []

        # Public trust roots require only the token projection; workspaces can share an engine.
        shared = copy.deepcopy(values)
        del shared["sql"]["credentials"][0]["caSecret"]
        second = copy.deepcopy(binding)
        second["workspaceId"] = "second"
        shared["sql"]["bindings"].append(second)
        shared["sql"]["engineEgress"].append({"cidr": "2001:db8::12/128", "port": 443})
        ds = render(shared)
        result = json.loads(next(d for d in ds if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "dataslate-sql")["data"]["bindings.json"])
        assert len(result) == 2 and all("caFile" not in b for b in result)
        c = next(d for d in ds if d["kind"] == "Deployment" and d["metadata"]["name"] == "dataslate-sql-controller")
        credential = next(v for v in c["spec"]["template"]["spec"]["volumes"] if v["name"] == "sql-credential-analytics")
        assert len(credential["projected"]["sources"]) == 1
        for p in (d for d in ds if d["kind"] == "NetworkPolicy"):
            for rule in p["spec"].get("egress", []):
                for peer in rule.get("to", []):
                    if "ipBlock" in peer:
                        ipaddress.ip_network(peer["ipBlock"]["cidr"], strict=False)

        # Managed PostgreSQL uses the same verified CA and scoped in-namespace peer as the API.
        managed_file = Path(__file__).resolve().parents[1] / "examples/eks-managed-postgres.yaml"
        managed = render(values, extra=("-f", managed_file, "--api-versions", "postgresql.cnpg.io/v1/Cluster"))
        managed_policy = next(d for d in managed if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "dataslate-sql-controller")
        assert managed_policy["spec"]["egress"][1]["to"] == [{"podSelector": {"matchLabels": {"cnpg.io/cluster": "dataslate-db"}}}]
        managed_pod = next(d for d in managed if d["kind"] == "Deployment" and d["metadata"]["name"] == "dataslate-sql-controller")["spec"]["template"]["spec"]
        assert next(e for e in managed_pod["containers"][0]["env"] if e["name"] == "DATASLATE_DATABASE_URL")["valueFrom"]["secretKeyRef"] == {"name": "dataslate-db-app", "key": "uri"}

        negatives = [
            (["enabled"], False, "require sql.enabled"),
            (["engineEgress"], [], "explicit sql.engineEgress"),
            (["credentials"], [], "credentialsId"),
            (["credentials", 0, "id"], "../outside", "id"),
            (["credentials", 0, "id"], "a" * 49, "id"),
            (["credentials", 0, "tokenSecret", "name"], "bad..name", "name"),
            (["credentials", 0, "tokenSecret", "name"], "", "name"),
            (["credentials", 0, "tokenSecret", "key"], "../token", "key"),
            (["credentials", 0, "tokenSecret", "value"], "plaintext", "value"),
            (["bindings", 0, "credentialsId"], "unknown", "credentialsId"),
            (["bindings", 0, "credentialsId"], "../outside", "credentialsId"),
            (["bindings", 0, "tokenFile"], "/etc/outside", "tokenFile"),
            (["bindings", 0, "caFile"], "/etc/outside", "caFile"),
            (["bindings", 0, "readOnlyPolicyAccepted"], False, "readOnlyPolicyAccepted"),
            (["bindings", 0, "policyVersion"], "", "policyVersion"),
            (["bindings", 0, "catalog"], "catalog\nX-Trino-User: root", "catalog"),
            (["bindings", 0, "maxRunSeconds"], 0, "maxRunSeconds"),
            (["bindings", 0, "maxRunSeconds"], 3601, "maxRunSeconds"),
            (["bindings", 0, "endpoint"], "http://trino.example.com", "endpoint"),
            (["bindings", 0, "endpoint"], "https://trino.example.com/v1", "endpoint"),
            (["bindings", 0, "endpoint"], "https://trino.example.com?token=secret", "endpoint"),
            (["bindings", 0, "endpoint"], "https://user:pass@trino.example.com", "endpoint"),
            (["bindings", 0, "allowedSubjects", 0, "issuer"], "https://wrong.example.com", "issuers must match"),
            (["bindings", 0, "allowedSubjects", 0, "engineUser"], "user\nroot", "engineUser"),
            (["engineEgress", 0, "cidr"], "0.0.0.0/0", "cidr"),
            (["engineEgress", 0, "cidr"], "169.254.169.254/32", "cidr"),
            (["engineEgress", 0, "cidr"], "127.0.0.1/32", "cidr"),
            (["engineEgress", 0, "cidr"], "::/0", "cidr"),
            (["engineEgress", 0, "cidr"], "10.0.999.1/32", "cidr"),
            (["engineEgress", 0, "cidr"], "2001:db8:::1/128", "cidr"),
            (["engineEgress", 0, "port"], 0, "port"),
            (["engineEgress", 0, "port"], 65536, "port"),
            (["controller", "replicas"], 0, "replicas"),
        ]
        for keys, value, error in negatives:
            invalid = copy.deepcopy(values)
            target = invalid["sql"]
            for key in keys[:-1]:
                if isinstance(target, dict): target = target.setdefault(key, {})
                else: target = target[key]
            target[keys[-1]] = value
            render(invalid, error)
        duplicate = copy.deepcopy(values)
        duplicate["sql"]["credentials"].append(copy.deepcopy(duplicate["sql"]["credentials"][0]))
        render(duplicate, "credential IDs must be unique")
        duplicate = copy.deepcopy(values)
        duplicate["sql"]["bindings"].append(copy.deepcopy(binding))
        render(duplicate, "binding IDs must be unique")
        duplicate = copy.deepcopy(values)
        subject = copy.deepcopy(binding["allowedSubjects"][0])
        subject["engineUser"] = "another_user"
        duplicate["sql"]["bindings"][0]["allowedSubjects"].append(subject)
        render(duplicate, "subject identities must be unique")
        subject["subject"] = "another-subject"
        subject["engineUser"] = "engineer_123"
        render(duplicate, "engine users must be distinct")
        print("PASS SQL deployment: isolated controller, scoped projected credentials, verified PostgreSQL, exact configured engine egress, revocation/drain and managed/external profiles")
        print(f"PASS SQL: {len(negatives) + 4} invalid/unsafe configuration rejection cases")

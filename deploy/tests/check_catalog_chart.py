"""Check actual Polaris metadata proxy renders, OAuth wiring and isolation."""
import copy
import json
from pathlib import Path
import tempfile

import yaml


def check_catalog_chart(run, external_args):
    binding = {
        "id": "lake", "workspaceId": "default", "name": "Lake catalog", "kind": "polaris", "policyVersion": "v1",
        "connection": {"endpoint": "https://polaris.example.com:8181/api/catalog", "warehouse": "lakehouse", "realm": "dataslate", "credentialsId": "catalog-reader"},
        "allowedSubjects": [{"issuer": "https://identity.example.com/realms/dataslate", "subject": "reader-123",
                             "tables": [{"namespace": ["analytics", "daily"], "name": "orders"}]}],
    }
    values = {"networkPolicy": {"httpsEgressCidrs": ["10.40.0.12/32"]}, "catalog": {"enabled": True, "bindings": [binding],
        "credentials": [{"id": "catalog-reader", "bearerTokenSecret": {"name": "polaris-reader", "key": "bearer"}, "caSecret": {"name": "polaris-ca", "key": "ca.pem"}}],
        "egress": [{"cidr": "10.50.0.12/32", "port": 8181}]}}
    with tempfile.TemporaryDirectory(prefix="dataslate-chart-catalog-") as directory:
        path = Path(directory) / "catalog.json"

        def render(value, error=None, extra=()):
            path.write_text(json.dumps(value))
            output = run([*external_args, "-f", path, *extra], error)
            return {(d["kind"], d["metadata"]["name"]): d for d in yaml.safe_load_all(output) if d} if output is not None else {}

        objects = render(values)
        assert not any(kind in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "Secret", "CustomResourceDefinition") for kind, _ in objects)
        config = objects[("ConfigMap", "dataslate-catalog")]
        expected = copy.deepcopy(binding)
        del expected["connection"]["credentialsId"]
        expected["connection"].update(auth={"bearerTokenFile": "/etc/dataslate/catalog-credentials/catalog-reader/token"}, caFile="/etc/dataslate/catalog-credentials/catalog-reader/ca.crt")
        assert json.loads(config["data"]["bindings.json"]) == [expected]
        assert "polaris-reader" not in json.dumps(config) and "ca.pem" not in json.dumps(config)
        api = objects[("Deployment", "dataslate-api")]["spec"]["template"]["spec"]
        assert api["automountServiceAccountToken"] is False
        assert api["securityContext"]["fsGroup"] == api["securityContext"]["runAsUser"] == 65532
        env = {e["name"]: e for e in api["containers"][0]["env"]}
        assert env["DATASLATE_CATALOG_BINDINGS_FILE"]["value"] == "/etc/dataslate/catalog/bindings.json"
        assert env["PGSSLMODE"]["value"] == "verify-full"
        volumes = {v["name"]: v for v in api["volumes"]}
        assert volumes["catalog-credential-catalog-reader"]["projected"] == {"defaultMode": 0o440, "sources": [
            {"secret": {"name": "polaris-reader", "items": [{"key": "bearer", "path": "token"}]}},
            {"secret": {"name": "polaris-ca", "items": [{"key": "ca.pem", "path": "ca.crt"}]}}]}
        mounts = api["containers"][0]["volumeMounts"]
        assert next(m for m in mounts if m["name"] == "catalog-credential-catalog-reader")["mountPath"] == "/etc/dataslate/catalog-credentials/catalog-reader"
        assert all(m.get("readOnly") is True and "subPath" not in m for m in mounts if m["name"].startswith("catalog"))
        for (kind, name), doc in objects.items():
            if kind not in ("Deployment", "Job") or name == "dataslate-api": continue
            assert not any(v["name"].startswith("catalog") for v in doc["spec"]["template"]["spec"].get("volumes", []))
        policy = objects[("NetworkPolicy", "dataslate-catalog")]["spec"]
        assert policy["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "api"
        assert policy["policyTypes"] == ["Egress"]
        assert policy["egress"] == [{"to": [{"ipBlock": {"cidr": "10.50.0.12/32"}}], "ports": [{"protocol": "TCP", "port": 8181}]}]
        assert "0.0.0.0/0" not in json.dumps([v for (k, _), v in objects.items() if k == "NetworkPolicy"])

        oauth = copy.deepcopy(values)
        credential = oauth["catalog"]["credentials"][0]
        del credential["bearerTokenSecret"]
        credential["oauth"] = {"clientId": "catalog-service", "clientSecret": {"name": "polaris-oauth", "key": "secret"},
                               "tokenEndpoint": "https://tokens.example.com:9443/oauth/token", "scope": "catalog.read", "style": "basic"}
        oauth["catalog"]["egress"].append({"cidr": "10.60.0.12/32", "port": 9443})
        rendered = render(oauth)
        b = json.loads(rendered[("ConfigMap", "dataslate-catalog")]["data"]["bindings.json"])[0]
        assert b["connection"]["auth"] == {"clientId": "catalog-service", "clientSecretFile": "/etc/dataslate/catalog-credentials/catalog-reader/client-secret",
                                            "tokenEndpoint": "https://tokens.example.com:9443/oauth/token", "scope": "catalog.read", "style": "basic"}
        v = next(v for v in rendered[("Deployment", "dataslate-api")]["spec"]["template"]["spec"]["volumes"] if v["name"] == "catalog-credential-catalog-reader")
        assert v["projected"]["sources"][0] == {"secret": {"name": "polaris-oauth", "items": [{"key": "secret", "path": "client-secret"}]}}
        assert len(rendered[("NetworkPolicy", "dataslate-catalog")]["spec"]["egress"]) == 2
        # Both kinds of private credentials stay in their own process when SQL is also enabled.
        sql_overlay = Path(__file__).resolve().parents[1] / "examples/eks-sql.yaml"
        combined = render(oauth, extra=("-f", sql_overlay))
        sql_pod = combined[("Deployment", "dataslate-sql-controller")]["spec"]["template"]["spec"]
        api_pod = combined[("Deployment", "dataslate-api")]["spec"]["template"]["spec"]
        assert not any(v["name"].startswith("catalog") for v in sql_pod["volumes"])
        assert not any(v["name"].startswith("sql-credential") for v in api_pod["volumes"])

        revoked = copy.deepcopy(values); revoked["catalog"]["bindings"][0]["allowedSubjects"] = []
        render(revoked)
        drained = copy.deepcopy(values); drained["catalog"]["bindings"] = []
        assert json.loads(render(drained)[("ConfigMap", "dataslate-catalog")]["data"]["bindings.json"]) == []
        minimal = copy.deepcopy(values); del minimal["catalog"]["credentials"][0]["caSecret"]
        assert "caFile" not in json.loads(render(minimal)[("ConfigMap", "dataslate-catalog")]["data"]["bindings.json"])[0]["connection"]
        negatives = [
            (["enabled"], False, "require catalog.enabled"),
            (["egress"], [], "explicit catalog.egress"),
            (["credentials"], [], "credentialsId"),
            (["credentials", 0, "id"], "../other", "id"),
            (["credentials", 0, "id"], "a" * 45, "id"),
            (["credentials", 0, "bearerTokenSecret", "key"], "../token", "key"),
            (["credentials", 0, "bearerTokenSecret", "name"], "bad..name", "name"),
            (["credentials", 0, "bearerTokenSecret", "value"], "plaintext", "value"),
            (["credentials", 0, "oauth"], credential["oauth"], "oneOf"),
            (["bindings", 0, "connection", "credentialsId"], "unknown", "credentialsId"),
            (["bindings", 0, "connection", "auth"], {"bearerTokenFile": "/etc/outside"}, "auth"),
            (["bindings", 0, "connection", "caFile"], "/etc/outside", "caFile"),
            (["bindings", 0, "connection", "endpoint"], "http://polaris.example.com", "endpoint"),
            (["bindings", 0, "connection", "endpoint"], "https://polaris.example.com/api/../other", "endpoint"),
            (["bindings", 0, "connection", "endpoint"], "https://polaris.example.com/api//catalog", "endpoint"),
            (["bindings", 0, "connection", "endpoint"], "https://user:pass@polaris.example.com/api", "endpoint"),
            (["bindings", 0, "connection", "endpoint"], "https://polaris.example.com/api?token=secret", "endpoint"),
            (["bindings", 0, "policyVersion"], "", "policyVersion"),
            (["bindings", 0, "allowedSubjects", 0, "issuer"], "https://another.example.com", "issuers must match"),
            (["bindings", 0, "allowedSubjects", 0, "subject"], "user\nroot", "subject"),
            (["bindings", 0, "allowedSubjects", 0, "tables", 0, "namespace"], [], "namespace"),
            (["bindings", 0, "allowedSubjects", 0, "tables", 0, "namespace"], ["a"] * 17, "namespace"),
            (["bindings", 0, "allowedSubjects", 0, "tables", 0, "name"], "..", "name"),
            (["bindings", 0, "allowedSubjects", 0, "tables", 0, "name"], "private/table", "name"),
            (["egress", 0, "cidr"], "0.0.0.0/0", "cidr"),
            (["egress", 0, "cidr"], "169.254.169.254/32", "cidr"),
            (["egress", 0, "port"], 0, "port"),
        ]
        for keys, value, error in negatives:
            invalid = copy.deepcopy(values); target = invalid["catalog"]
            for key in keys[:-1]: target = target[key]
            target[keys[-1]] = value
            render(invalid, error)
        for cidr in ["0.0.0.0/0", "::/0"]:
            invalid = copy.deepcopy(values); invalid["networkPolicy"]["httpsEgressCidrs"] = [cidr]
            render(invalid, "scoped networkPolicy.httpsEgressCidrs")
        for key, value in [("tokenEndpoint", "http://tokens.example.com/oauth"), ("style", "unsafe"), ("scope", "read\nwrite"), ("clientId", "")]:
            invalid = copy.deepcopy(oauth); invalid["catalog"]["credentials"][0]["oauth"][key] = value
            render(invalid, key)
        invalid = copy.deepcopy(values); invalid["catalog"]["credentials"].append(copy.deepcopy(values["catalog"]["credentials"][0])); render(invalid, "credential IDs must be unique")
        invalid = copy.deepcopy(values); invalid["catalog"]["bindings"].append(copy.deepcopy(binding)); render(invalid, "binding IDs must be unique")
        invalid = copy.deepcopy(values); subject = copy.deepcopy(binding["allowedSubjects"][0]); subject["tables"] = []; invalid["catalog"]["bindings"][0]["allowedSubjects"].append(subject); render(invalid, "subject identities must be unique")
        print("PASS catalog deployment: bearer/OAuth projection, API-only credentials, SQL separation, scoped IdP and catalog egress, grant revocation")
        print(f"PASS catalog: {len(negatives) + 9} invalid/unsafe configuration rejection cases")

"""Validate worker-only model credentials and exact context-profile/egress wiring."""
import copy
import json
from pathlib import Path
import tempfile

import yaml


def check_sylon_chart(run, external_args, managed_args):
    root = Path(__file__).resolve().parents[1]
    values = yaml.safe_load((root / "examples/eks-sylon.yaml").read_text())
    binding = values["sylon"]["bindings"][0]
    with tempfile.TemporaryDirectory(prefix="dataslate-chart-sylon-") as directory:
        path = Path(directory) / "sylon.json"
        def render(value, error=None, args=external_args, extra=()):
            path.write_text(json.dumps(value)); output = run([*args, "-f", path, *extra], error)
            return {(d["kind"], d["metadata"]["name"]): d for d in yaml.safe_load_all(output) if d} if output is not None else {}
        def pod(objects, name): return objects[("Deployment", "dataslate-" + name)]["spec"]["template"]["spec"]
        def bindings(objects, name): return json.loads(objects[("ConfigMap", "dataslate-" + name)]["data"]["bindings.json"])
        def credentials(p): return [v for v in p["volumes"] if "credential" in v["name"]]
        def env(p): return {e["name"]: e for e in p["containers"][0]["env"]}
        objects = render(values); worker = pod(objects, "sylon-controller"); api = pod(objects, "api")
        assert not any(kind in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "Secret", "CustomResourceDefinition") for kind, _ in objects)
        assert not credentials(api) and not credentials(pod(objects, "web"))
        assert env(api)["DATASLATE_SYLON_BINDINGS_FILE"]["value"] == env(worker)["DATASLATE_SYLON_BINDINGS_FILE"]["value"] == "/etc/dataslate/sylon/bindings.json"
        b = bindings(objects, "sylon")[0]; expected = copy.deepcopy(binding); del expected["provider"]["credentialsId"]
        expected["provider"].update(auth={"bearerTokenFile": "/etc/dataslate/sylon-credentials/assistant/token"}, caFile="/etc/dataslate/sylon-credentials/assistant/ca.crt")
        assert b == expected
        assert worker["automountServiceAccountToken"] is False
        assert worker["securityContext"]["fsGroup"] == worker["securityContext"]["runAsUser"] == 65532
        sec = worker["containers"][0]["securityContext"]
        assert sec["readOnlyRootFilesystem"] is True and sec["capabilities"]["drop"] == ["ALL"] and sec["allowPrivilegeEscalation"] is False
        assert "dataslate.io/tier" not in objects[("Deployment", "dataslate-sylon-controller")]["spec"]["template"]["metadata"]["labels"]
        assert env(worker)["DATASLATE_SYLON_CONTROLLER_ADDR"]["value"] == "0.0.0.0:8084"
        assert env(worker)["PGSSLMODE"]["value"] == "verify-full"
        assert worker["containers"][0]["ports"][0]["containerPort"] == 8084
        assert credentials(worker)[0]["projected"] == {"defaultMode": 0o440, "sources": [
            {"secret": {"name": "dataslate-model-provider", "items": [{"key": "token", "path": "token"}]}},
            {"secret": {"name": "dataslate-model-ca", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}
        mounts = worker["containers"][0]["volumeMounts"]
        assert next(m for m in mounts if m["name"] == "sylon")["mountPath"] == "/etc/dataslate/sylon"
        assert all(m.get("readOnly") and "subPath" not in m for m in mounts if m["name"].startswith("sylon"))
        policy = objects[("NetworkPolicy", "dataslate-sylon-controller")]["spec"]
        assert policy["ingress"] == [] and len(policy["egress"]) == 3
        assert "0.0.0.0/0" not in json.dumps(policy)
        assert policy["egress"][-1] == {"to": [{"ipBlock": {"cidr": "10.80.0.12/32"}}], "ports": [{"protocol": "TCP", "port": 443}]}
        assert ("NetworkPolicy", "dataslate-sylon-api") not in objects
        managed = render(values, args=managed_args)
        assert managed[("NetworkPolicy", "dataslate-sylon-controller")]["spec"]["egress"][1]["to"] == [{"podSelector": {"matchLabels": {"cnpg.io/cluster": "dataslate-db"}}}]
        oauth = copy.deepcopy(values); c = oauth["sylon"]["credentials"][0]; del c["bearerTokenSecret"]
        c["oauth"] = {"clientId": "assistant-client", "clientSecret": {"name": "model-oauth", "key": "secret"}, "tokenEndpoint": "https://tokens.example.com/oauth/token", "scope": "model.generate", "style": "post"}
        oauth["sylon"]["providerEgress"].append({"cidr": "10.80.0.13/32", "port": 443})
        o = render(oauth); assert bindings(o, "sylon")[0]["provider"]["auth"] == {"clientId": "assistant-client", "clientSecretFile": "/etc/dataslate/sylon-credentials/assistant/client-secret", "tokenEndpoint": "https://tokens.example.com/oauth/token", "scope": "model.generate", "style": "post"}
        assert credentials(pod(o, "sylon-controller"))[0]["projected"]["sources"][0] == {"secret": {"name": "model-oauth", "items": [{"key": "secret", "path": "client-secret"}]}}
        chat = copy.deepcopy(values); chat["sylon"]["bindings"][0]["provider"]["protocol"] = "chat-completions"; del chat["sylon"]["credentials"][0]["caSecret"]
        assert "caFile" not in bindings(render(chat), "sylon")[0]["provider"]

        combined = copy.deepcopy(values)
        combined.update(yaml.safe_load((root / "examples/eks-search.yaml").read_text()))
        combined["catalog"] = yaml.safe_load((root / "examples/eks-catalog.yaml").read_text())["catalog"]
        search = combined["search"]["bindings"][0]; catalog = combined["catalog"]["bindings"][0]
        combined["sylon"]["bindings"][0]["context"]["searchBindingIds"] = [search["id"]]
        # No direct catalog context: retrieval's catalog source still requires its exact authority hash.
        search["sources"]["catalogs"] = [{"bindingId": catalog["id"], "tables": [{"namespace": ["analytics"], "name": "orders"}]}]
        combined["search"]["catalogEgress"] = copy.deepcopy(combined["catalog"]["egress"])
        combined["sylon"]["searchEgress"] = copy.deepcopy(combined["search"]["embeddingEgress"])
        combined["sylon"]["catalogEgress"] = copy.deepcopy(combined["catalog"]["egress"])
        other = copy.deepcopy(search); other["id"] = "unrelated-search"; other["embedding"]["credentialsId"] = "other-search"; combined["search"]["bindings"].append(other)
        combined["search"]["credentials"].append({"id": "other-search", "bearerTokenSecret": {"name": "unrelated-embed", "key": "token"}})
        other = copy.deepcopy(catalog); other["id"] = "unrelated-catalog"; other["connection"]["credentialsId"] = "other-catalog"; combined["catalog"]["bindings"].append(other)
        combined["catalog"]["credentials"].append({"id": "other-catalog", "bearerTokenSecret": {"name": "unrelated-catalog", "key": "token"}})
        full = render(combined, extra=("-f", root / "examples/eks-sql.yaml"))
        assert bindings(full, "sylon-search") == [bindings(full, "search")[0]]
        assert bindings(full, "sylon-catalog") == [bindings(full, "catalog")[0]]
        p = pod(full, "sylon-controller"); names = [v["name"] for v in credentials(p)]
        assert "search-credential-" + search["embedding"]["credentialsId"] in names
        assert "catalog-credential-" + catalog["connection"]["credentialsId"] in names
        assert "search-credential-other-search" not in names and "catalog-credential-other-catalog" not in names
        assert not any(n.startswith("sql") for n in names)
        for name in ("api", "web", "sql-controller", "search-controller"):
            assert not any(v["name"].startswith("sylon-credential") for v in pod(full, name)["volumes"])
        assert len(full[("NetworkPolicy", "dataslate-sylon-controller")]["spec"]["egress"]) == 5
        direct = copy.deepcopy(combined); direct["sylon"]["bindings"][0]["context"].update(searchBindingIds=[], catalogBindingIds=[catalog["id"]])
        direct_objects = render(direct); assert ("ConfigMap", "dataslate-sylon-search") not in direct_objects
        assert bindings(direct_objects, "sylon-catalog") == [bindings(direct_objects, "catalog")[0]]
        revoked = copy.deepcopy(combined); revoked["sylon"]["bindings"][0]["allowedSubjects"] = []; render(revoked)
        revoked["sylon"]["bindings"] = []; r = render(revoked)
        assert bindings(r, "sylon") == [] and ("ConfigMap", "dataslate-sylon-search") not in r and ("ConfigMap", "dataslate-sylon-catalog") not in r
        assert not any(v["name"].startswith(("search", "catalog")) for v in credentials(pod(r, "sylon-controller")))

        negatives = [
            (["enabled"], False, "require sylon.enabled"),
            (["credentials"], [], "credentialsId"),
            (["credentials", 0, "id"], "../outside", "id"),
            (["credentials", 0, "id"], "a" * 45, "id"),
            (["credentials", 0, "bearerTokenSecret", "key"], "../secret", "key"),
            (["credentials", 0, "bearerTokenSecret", "value"], "raw-key", "value"),
            (["credentials", 0, "oauth"], c["oauth"], "oneOf"),
            (["bindings", 0, "name"], " ", "must not be blank"),
            (["bindings", 0, "policyVersion"], "", "policyVersion"),
            (["bindings", 0, "dataTransferAccepted"], False, "dataTransferAccepted"),
            (["bindings", 0, "maxContextBytes"], 98305, "maxContextBytes"),
            (["bindings", 0, "maxContextBytes"], 1023, "maxContextBytes"),
            (["bindings", 0, "provider", "credentialsId"], "unknown", "credentialsId"),
            (["bindings", 0, "provider", "protocol"], "arbitrary", "protocol"),
            (["bindings", 0, "provider", "endpoint"], "http://model.example.com", "endpoint"),
            (["bindings", 0, "provider", "endpoint"], "https://user:secret@model.example.com", "endpoint"),
            (["bindings", 0, "provider", "endpoint"], "https://model.example.com/../other", "endpoint"),
            (["bindings", 0, "provider", "endpoint"], "https://model.example.com/v1?api_key=bad", "endpoint"),
            (["bindings", 0, "provider", "model"], "bad\nmodel", "model"),
            (["bindings", 0, "provider", "maxOutputTokens"], 8193, "maxOutputTokens"),
            (["bindings", 0, "provider", "maxOutputTokens"], 255, "maxOutputTokens"),
            (["bindings", 0, "provider", "auth"], {"bearerTokenFile": "/etc/private"}, "auth"),
            (["bindings", 0, "provider", "caFile"], "/etc/private", "caFile"),
            (["bindings", 0, "allowedSubjects", 0, "issuer"], "https://other.example.com", "issuers must match"),
            (["bindings", 0, "allowedSubjects", 0, "subject"], "user\nroot", "subject"),
            (["bindings", 0, "context", "catalogBindingIds"], ["missing"], "enabled catalog binding"),
            (["bindings", 0, "context", "searchBindingIds"], ["missing"], "enabled search binding"),
            (["providerEgress"], [], "explicit sylon.providerEgress"),
            (["providerEgress", 0, "cidr"], "0.0.0.0/0", "cidr"),
            (["providerEgress", 0, "cidr"], "169.254.169.254/32", "cidr"),
            (["providerEgress", 0, "port"], 0, "port"),
        ]
        for keys, value, error in negatives:
            invalid = copy.deepcopy(values); target = invalid["sylon"]
            for key in keys[:-1]: target = target[key]
            target[keys[-1]] = value; render(invalid, error)
        for key, value in [("tokenEndpoint", "http://tokens.example.com/token"), ("style", "unsafe"), ("clientId", ""), ("scope", "model\nadmin")]:
            invalid = copy.deepcopy(oauth); invalid["sylon"]["credentials"][0]["oauth"][key] = value; render(invalid, key)
        invalid = copy.deepcopy(values); invalid["sylon"]["bindings"].append(copy.deepcopy(binding)); render(invalid, "binding IDs must be unique")
        invalid = copy.deepcopy(values); invalid["sylon"]["credentials"].append(copy.deepcopy(invalid["sylon"]["credentials"][0])); render(invalid, "credential IDs must be unique")
        invalid = copy.deepcopy(values); invalid["sylon"]["bindings"][0]["allowedSubjects"] *= 2; render(invalid, "allowedSubjects")
        invalid = copy.deepcopy(combined); invalid["sylon"]["catalogEgress"] = []; render(invalid, "explicit sylon.catalogEgress")
        invalid = copy.deepcopy(combined); invalid["sylon"]["searchEgress"] = []; render(invalid, "explicit sylon.searchEgress")
        invalid = copy.deepcopy(combined); invalid["sylon"]["bindings"][0]["workspaceId"] = "another-team"; render(invalid, "same workspace")
        print("PASS Sylon deployment: provider credentials worker-only, explicit subject/context authority, selected indirect catalog/search credentials, exact egress, OAuth/bearer and revocation")
        print(f"PASS Sylon: {len(negatives) + 10} invalid/unsafe configuration rejection cases")

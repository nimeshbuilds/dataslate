"""Render actual isolated search workers, credentials, and exact network policy."""
import copy
import json
from pathlib import Path
import tempfile

import yaml


def check_search_chart(run, external_args, managed_args):
    binding = {"id": "discovery", "workspaceId": "default", "name": "Discovery", "policyVersion": "v1", "mode": "lexical", "maxDocuments": 1000,
               "sources": {"notebooks": True, "includeCellSource": False, "catalogs": []}, "dataTransferAccepted": False}
    values = {"search": {"enabled": True, "bindings": [binding]}}
    with tempfile.TemporaryDirectory(prefix="dataslate-chart-search-") as directory:
        path = Path(directory) / "search.json"
        def render(value, error=None, args=external_args, extra=()):
            path.write_text(json.dumps(value))
            output = run([*args, "-f", path, *extra], error)
            return {(d["kind"], d["metadata"]["name"]): d for d in yaml.safe_load_all(output) if d} if output is not None else {}
        def pod(objects, name): return objects[("Deployment", "dataslate-" + name)]["spec"]["template"]["spec"]
        def bindings(objects, name): return json.loads(objects[("ConfigMap", "dataslate-" + name)]["data"]["bindings.json"])
        def credentials(p): return [v for v in p["volumes"] if "credential" in v["name"]]
        lexical = render(values)
        assert bindings(lexical, "search") == [binding]
        assert not any(kind in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "Secret", "CustomResourceDefinition") for kind, _ in lexical)
        assert ("ConfigMap", "dataslate-search-catalog") not in lexical and ("NetworkPolicy", "dataslate-search-api") not in lexical
        for name in ("api", "search-controller"):
            p = pod(lexical, name); c = p["containers"][0]
            assert p["automountServiceAccountToken"] is False
            assert p["securityContext"]["runAsUser"] == p["securityContext"]["fsGroup"] == 65532
            assert c["securityContext"]["readOnlyRootFilesystem"] is True
            assert c["securityContext"]["capabilities"]["drop"] == ["ALL"]
            env = {e["name"]: e for e in c["env"]}
            assert env["DATASLATE_SEARCH_BINDINGS_FILE"]["value"] == "/etc/dataslate/search/bindings.json"
            assert env["PGSSLMODE"]["value"] == "verify-full"
            assert next(m for m in c["volumeMounts"] if m["name"] == "search")["mountPath"] == "/etc/dataslate/search"
            assert not credentials(p)
        worker = pod(lexical, "search-controller"); env = {e["name"]: e for e in worker["containers"][0]["env"]}
        assert env["DATASLATE_SEARCH_CONTROLLER_ADDR"]["value"] == "0.0.0.0:8083"
        assert worker["containers"][0]["ports"] == [{"name": "http", "containerPort": 8083, "protocol": "TCP"}]
        labels = lexical[("Deployment", "dataslate-search-controller")]["spec"]["template"]["metadata"]["labels"]
        assert "dataslate.io/tier" not in labels
        policy = lexical[("NetworkPolicy", "dataslate-search-controller")]["spec"]
        assert policy["ingress"] == [] and len(policy["egress"]) == 2
        assert "0.0.0.0/0" not in json.dumps(policy)
        managed = render(values, args=managed_args)
        assert managed[("NetworkPolicy", "dataslate-search-controller")]["spec"]["egress"][1]["to"] == [{"podSelector": {"matchLabels": {"cnpg.io/cluster": "dataslate-db"}}}]

        hybrid = copy.deepcopy(values)
        hybrid["networkPolicy"] = {"httpsEgressCidrs": ["10.40.0.12/32"]}
        hybrid["search"]["bindings"][0].update(mode="hybrid", dataTransferAccepted=True, embedding={"endpoint": "https://embeddings.example.com/v1/embeddings", "model": "approved-embedding", "dimensions": 1024, "credentialsId": "embedding"})
        hybrid["search"].update(credentials=[{"id": "embedding", "bearerTokenSecret": {"name": "gateway-token", "key": "access-token"}, "caSecret": {"name": "gateway-ca", "key": "trust.pem"}}], embeddingEgress=[{"cidr": "10.70.0.12/32", "port": 443}])
        objects = render(hybrid); b = bindings(objects, "search")[0]
        assert b["embedding"]["auth"] == {"bearerTokenFile": "/etc/dataslate/search-credentials/embedding/token"}
        assert b["embedding"]["caFile"] == "/etc/dataslate/search-credentials/embedding/ca.crt"
        assert "credentialsId" not in b["embedding"]
        assert "access-token" not in json.dumps(objects[("ConfigMap", "dataslate-search")])
        for name in ("api", "search-controller"):
            p = pod(objects, name)
            assert credentials(p)[0]["projected"] == {"defaultMode": 0o440, "sources": [{"secret": {"name": "gateway-token", "items": [{"key": "access-token", "path": "token"}]}}, {"secret": {"name": "gateway-ca", "items": [{"key": "trust.pem", "path": "ca.crt"}]}}]}
            assert all(m.get("readOnly") and "subPath" not in m for m in p["containers"][0]["volumeMounts"] if m["name"].startswith("search"))
        api_policy = objects[("NetworkPolicy", "dataslate-search-api")]["spec"]
        assert api_policy["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "api"
        assert api_policy["egress"] == [{"to": [{"ipBlock": {"cidr": "10.70.0.12/32"}}], "ports": [{"protocol": "TCP", "port": 443}]}]
        assert len(objects[("NetworkPolicy", "dataslate-search-controller")]["spec"]["egress"]) == 3
        assert not credentials(pod(objects, "web"))

        oauth = copy.deepcopy(hybrid); credential = oauth["search"]["credentials"][0]; del credential["bearerTokenSecret"]
        credential["oauth"] = {"clientId": "discovery-worker", "clientSecret": {"name": "gateway-oauth", "key": "client-secret"}, "tokenEndpoint": "https://tokens.example.com/oauth/token", "scope": "embed", "style": "basic"}
        oauth["search"]["embeddingEgress"].append({"cidr": "10.70.0.13/32", "port": 443})
        o = render(oauth); auth = bindings(o, "search")[0]["embedding"]["auth"]
        assert auth == {"clientId": "discovery-worker", "clientSecretFile": "/etc/dataslate/search-credentials/embedding/client-secret", "tokenEndpoint": "https://tokens.example.com/oauth/token", "scope": "embed", "style": "basic"}
        assert credentials(pod(o, "search-controller"))[0]["projected"]["sources"][0] == {"secret": {"name": "gateway-oauth", "items": [{"key": "client-secret", "path": "client-secret"}]}}
        minimal = copy.deepcopy(hybrid); del minimal["search"]["credentials"][0]["caSecret"]
        assert "caFile" not in bindings(render(minimal), "search")[0]["embedding"]

        combined = copy.deepcopy(hybrid)
        combined["catalog"] = yaml.safe_load((Path(__file__).resolve().parents[1] / "examples/eks-catalog.yaml").read_text())["catalog"]
        cb = combined["catalog"]["bindings"][0]; cid = cb["id"]
        combined["search"]["bindings"][0]["sources"]["catalogs"] = [{"bindingId": cid, "tables": [{"namespace": ["analytics"], "name": "orders"}]}]
        combined["search"]["catalogEgress"] = copy.deepcopy(combined["catalog"]["egress"])
        other = copy.deepcopy(cb); other["id"] = "unrelated-catalog"; other["connection"]["credentialsId"] = "unrelated"
        combined["catalog"]["bindings"].append(other)
        combined["catalog"]["credentials"].append({"id": "unrelated", "bearerTokenSecret": {"name": "private-catalog", "key": "token"}})
        combined_objects = render(combined, extra=("-f", Path(__file__).resolve().parents[1] / "examples/eks-sql.yaml"))
        assert bindings(combined_objects, "search-catalog") == [bindings(combined_objects, "catalog")[0]]
        p = pod(combined_objects, "search-controller")
        names = [v["name"] for v in credentials(p)]
        assert "catalog-credential-unrelated" not in names
        assert "catalog-credential-" + cb["connection"]["credentialsId"] in names
        assert not any(n.startswith("sql") for n in names)
        assert not any(v["name"].startswith("search") for v in pod(combined_objects, "sql-controller")["volumes"])
        assert not any(v["name"].startswith("sql-credential") for v in pod(combined_objects, "api")["volumes"])
        assert len(combined_objects[("NetworkPolicy", "dataslate-search-controller")]["spec"]["egress"]) == 2 + len(combined["search"]["embeddingEgress"]) + len(combined["search"]["catalogEgress"])
        revoked = copy.deepcopy(combined); revoked["search"]["bindings"] = []
        revoked_objects = render(revoked)
        assert bindings(revoked_objects, "search") == []
        assert ("ConfigMap", "dataslate-search-catalog") not in revoked_objects
        assert not any(v["name"].startswith("catalog") for v in pod(revoked_objects, "search-controller")["volumes"])

        negatives = [
            (["enabled"], False, "require search.enabled"),
            (["credentials"], [], "credentialsId"),
            (["credentials", 0, "id"], "../outside", "id"),
            (["credentials", 0, "id"], "a" * 45, "id"),
            (["credentials", 0, "bearerTokenSecret", "key"], "../secret", "key"),
            (["credentials", 0, "bearerTokenSecret", "value"], "plaintext", "value"),
            (["credentials", 0, "oauth"], credential["oauth"], "oneOf"),
            (["bindings", 0, "policyVersion"], "", "policyVersion"),
            (["bindings", 0, "name"], " ", "must not be blank"),
            (["bindings", 0, "maxDocuments"], 1001, "maxDocuments"),
            (["bindings", 0, "dataTransferAccepted"], False, "dataTransferAccepted"),
            (["bindings", 0, "embedding", "credentialsId"], "unknown", "credentialsId"),
            (["bindings", 0, "embedding", "endpoint"], "http://gateway.example.com", "endpoint"),
            (["bindings", 0, "embedding", "endpoint"], "https://user:pass@gateway.example.com", "endpoint"),
            (["bindings", 0, "embedding", "endpoint"], "https://gateway.example.com/../outside", "endpoint"),
            (["bindings", 0, "embedding", "endpoint"], "https://gateway.example.com/v1?token=secret", "endpoint"),
            (["bindings", 0, "embedding", "model"], "model\nother", "model"),
            (["bindings", 0, "embedding", "dimensions"], 2001, "dimensions"),
            (["bindings", 0, "embedding", "auth"], {"bearerTokenFile": "/etc/private"}, "auth"),
            (["bindings", 0, "embedding", "caFile"], "/etc/private", "caFile"),
            (["bindings", 0, "sources", "catalogs"], [{"bindingId": "missing", "tables": []}], "require catalog.enabled"),
            (["embeddingEgress"], [], "explicit search.embeddingEgress"),
            (["embeddingEgress", 0, "cidr"], "0.0.0.0/0", "cidr"),
            (["embeddingEgress", 0, "cidr"], "169.254.169.254/32", "cidr"),
            (["embeddingEgress", 0, "port"], 0, "port"),
        ]
        for keys, value, error in negatives:
            invalid = copy.deepcopy(hybrid); target = invalid["search"]
            for key in keys[:-1]: target = target[key]
            target[keys[-1]] = value; render(invalid, error)
        for cidr in ["0.0.0.0/0", "::/0"]:
            invalid = copy.deepcopy(hybrid); invalid["networkPolicy"]["httpsEgressCidrs"] = [cidr]; render(invalid, "scoped networkPolicy.httpsEgressCidrs")
        for key, value in [("tokenEndpoint", "http://tokens.example.com/token"), ("style", "unsafe"), ("clientId", ""), ("scope", "embed\nadmin")]:
            invalid = copy.deepcopy(oauth); invalid["search"]["credentials"][0]["oauth"][key] = value; render(invalid, key)
        invalid = copy.deepcopy(values); invalid["search"]["bindings"][0]["dataTransferAccepted"] = True; render(invalid, "dataTransferAccepted")
        invalid = copy.deepcopy(values); invalid["search"]["bindings"][0]["embedding"] = hybrid["search"]["bindings"][0]["embedding"]; render(invalid, "not")
        invalid = copy.deepcopy(values); invalid["search"]["bindings"][0]["sources"].update(notebooks=False, includeCellSource=True); render(invalid, "includeCellSource")
        invalid = copy.deepcopy(hybrid); invalid["search"]["credentials"].append(copy.deepcopy(invalid["search"]["credentials"][0])); render(invalid, "credential IDs must be unique")
        invalid = copy.deepcopy(values); invalid["search"]["bindings"].append(copy.deepcopy(binding)); render(invalid, "binding IDs must be unique")
        invalid = copy.deepcopy(combined); invalid["search"]["bindings"][0]["sources"]["catalogs"][0]["bindingId"] = "missing"; render(invalid, "same workspace")
        invalid = copy.deepcopy(combined); invalid["search"]["catalogEgress"] = []; render(invalid, "explicit search.catalogEgress")
        invalid = copy.deepcopy(combined); invalid["search"]["bindings"][0]["sources"]["catalogs"] *= 2; render(invalid, "source binding IDs must be unique")
        invalid = copy.deepcopy(combined); invalid["search"]["bindings"][0]["sources"]["catalogs"][0]["tables"] = [{"namespace": [], "name": "orders"}]; render(invalid, "namespace")
        print("PASS search deployment: lexical/hybrid, external/managed database, OAuth/bearer projection, API/worker egress, selected catalog credentials, SQL isolation")
        print(f"PASS search: {len(negatives) + 15} invalid/unsafe configuration rejection cases")

"""Exercise optional issuer trust against actual Helm renders."""
import json

import yaml


def check_oidc_chart(run, external_args):
    def render(extra=()):
        return [d for d in yaml.safe_load_all(run([*external_args, *extra])) if d]

    def pods(docs):
        return [(d["metadata"]["name"], d["spec"]["template"]["spec"])
                for d in docs if d["kind"] in ("Deployment", "Job")]

    for _, pod in pods(render()):
        assert all(v["name"] != "oidc-ca" for v in pod.get("volumes", []))
        assert all(e["name"] != "DATASLATE_OIDC_CA_FILE"
                   for c in pod["containers"] for e in c.get("env", []))

    docs = render(["--set-json", 'oidc.caSecret={"name":"organization.identity-ca","key":"roots.pem"}'])
    assert not any(d["kind"] == "Secret" for d in docs), "External issuer trust must not be adopted or created"
    for name, pod in pods(docs):
        volumes = [v for v in pod.get("volumes", []) if v["name"] == "oidc-ca"]
        mounts = [m for c in pod["containers"] for m in c.get("volumeMounts", []) if m["name"] == "oidc-ca"]
        env = [e for c in pod["containers"] for e in c.get("env", []) if e["name"] == "DATASLATE_OIDC_CA_FILE"]
        if name == "dataslate-api":
            assert volumes == [{"name": "oidc-ca", "secret": {"secretName": "organization.identity-ca", "defaultMode": 0o444,
                                   "items": [{"key": "roots.pem", "path": "ca.crt"}]}}]
            assert mounts == [{"name": "oidc-ca", "mountPath": "/etc/dataslate/oidc", "readOnly": True}]
            assert env == [{"name": "DATASLATE_OIDC_CA_FILE", "value": "/etc/dataslate/oidc/ca.crt"}]
        else:
            assert not volumes and not mounts and not env, "Issuer trust leaked to a non-API workload"

    invalid = [
        {}, {"name": "issuer-ca"}, {"key": "ca.crt"}, {"name": "", "key": "ca.crt"},
        {"name": "issuer/ca", "key": "ca.crt"}, {"name": "IssuerCA", "key": "ca.crt"},
        {"name": "issuer..ca", "key": "ca.crt"}, {"name": "-issuer", "key": "ca.crt"},
        {"name": "x" * 64, "key": "ca.crt"}, {"name": "issuer-ca", "key": ""},
        {"name": "issuer-ca", "key": "../ca.crt"}, {"name": "issuer-ca", "key": "ca crt"},
        {"name": "issuer-ca", "key": "ca.crt", "insecureSkipVerify": True},
        {"name": "issuer-ca", "key": "ca.crt", "value": "not-a-secret-value"},
    ]
    for value in invalid:
        run([*external_args, "--set-json", "oidc.caSecret=" + json.dumps(value)], "caSecret")
    print(f"PASS OIDC trust: API-only existing Secret projection, default system trust, {len(invalid)} invalid references rejected")

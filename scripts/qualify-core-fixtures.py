#!/usr/bin/env python3
"""Generate private, disposable core-install fixtures; never contact a cluster.

Not a production installer. Use only in a separately owned qualification cluster.
The output contains generated credentials and must never be committed or logged.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess

NS = "dataslate-core-fixture"
APP_NS = "dataslate-core-test"
ISSUER = "https://identity.dataslate.test:18443/realms/dataslate"
APP = "https://workspace.dataslate.test:18444"
IMAGES = {
    "keycloak": "quay.io/keycloak/keycloak:26.8.2@sha256:3879c623248408de29a5303178b5816c77e8dfef3890c361f8c0bbeaa979ba3b",
    "postgres": "docker.io/library/postgres:17.11-bookworm@sha256:665dc7b37ac52a17e298511414c21d23ffa4e6a6ef6225b3e5d59c7afb35e994",
}


def generate(out):
    out.mkdir(mode=0o700, parents=False, exist_ok=False)
    os.chmod(out, 0o700)

    def write(name, value):
        path = out / name
        with path.open("x") as f:
            os.chmod(path, 0o600)
            f.write(value)

    def openssl(*args):
        subprocess.run(["openssl", *args], check=True, cwd=out,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)

    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-keyout", "ca.key",
            "-out", "ca.crt", "-subj", "/CN=DataSlate disposable qualification CA")
    sans = ["identity.dataslate.test", "workspace.dataslate.test", "postgres",
            f"postgres.{NS}", f"postgres.{NS}.svc", f"postgres.{NS}.svc.cluster.local"]
    write("server.ext", "subjectAltName=" + ",".join("DNS:" + x for x in sans) +
          "\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n")
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr",
            "-subj", "/CN=DataSlate disposable qualification server")
    openssl("x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
            "-out", "server.crt", "-days", "2", "-extfile", "server.ext")
    for path in out.iterdir():
        path.chmod(0o600)
    passwords = {name: secrets.token_hex(24) for name in
                 ["postgres", "migrator", "runtime", "keycloak", "admin", "editor", "viewer", "outsider"]}
    write("credentials.json", json.dumps(passwords))
    items = []

    def obj(kind, name, **fields):
        api = "apps/v1" if kind == "Deployment" else "v1"
        return {"apiVersion": api, "kind": kind, "metadata": {"name": name, "namespace": NS}, **fields}

    def secret(name, data, namespace=NS):
        item = obj("Secret", name, type="Opaque", data={k: base64.b64encode(v.encode()).decode() for k, v in data.items()})
        item["metadata"]["namespace"] = namespace
        items.append(item)

    for ns in [NS, APP_NS]:
        items.append({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns,
                      "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
    ca = (out / "ca.crt").read_text()
    crt = (out / "server.crt").read_text()
    key = (out / "server.key").read_text()
    secret("fixture-tls", {"ca.crt": ca, "tls.crt": crt, "tls.key": key})
    secret("fixture-ca", {"ca.crt": ca}, APP_NS)
    secret("fixture-web-tls", {"tls.crt": crt, "tls.key": key}, APP_NS)
    host = f"postgres.{NS}.svc.cluster.local"
    for role in ["migrator", "runtime"]:
        url = f"postgresql://dataslate_{role}:{passwords[role]}@{host}:5432/dataslate?sslmode=verify-full"
        secret("dataslate-" + role, {"database-url": url}, APP_NS)
    sql = "\n".join([
        f"CREATE ROLE dataslate_migrator LOGIN PASSWORD '{passwords['migrator']}';",
        f"CREATE ROLE dataslate_runtime LOGIN PASSWORD '{passwords['runtime']}';",
        "CREATE DATABASE dataslate OWNER dataslate_migrator;",
        "REVOKE ALL ON DATABASE dataslate FROM PUBLIC;",
        f"CREATE ROLE dataslate_idp LOGIN PASSWORD '{passwords['keycloak']}';",
        "CREATE DATABASE keycloak OWNER dataslate_idp;",
        "REVOKE ALL ON DATABASE keycloak FROM PUBLIC;",
    ])
    secret("postgres-init", {"admin-password": passwords["postgres"], "01-roles.sql": sql})
    secret("keycloak-db", {"password": passwords["keycloak"]})

    def mapper(name, mapper_type, config):
        return {"name": name, "protocol": "openid-connect", "protocolMapper": mapper_type,
                "consentRequired": False, "config": config}

    claim = {"access.token.claim": "true", "id.token.claim": "false", "userinfo.token.claim": "false"}
    mappers = [mapper("api-audience", "oidc-audience-mapper", {
        "included.custom.audience": "dataslate-api", "access.token.claim": "true", "id.token.claim": "false"})]
    for name in ["dataslate_workspaces", "dataslate_roles"]:
        mappers.append(mapper(name, "oidc-usermodel-attribute-mapper", claim | {
            "user.attribute": name, "claim.name": name, "jsonType.label": "JSON", "multivalued": "false"}))
    users = []
    for name, memberships in [("admin", {"team-alpha": "admin"}), ("editor", {"team-alpha": "editor"}),
                              ("viewer", {"team-alpha": "viewer"}), ("outsider", {"team-beta": "admin"})]:
        users.append({"username": name, "enabled": True, "emailVerified": True,
                      "firstName": name.title(), "lastName": "Fixture", "email": name + "@example.invalid",
                      "attributes": {"dataslate_workspaces": [json.dumps(memberships)],
                                     "dataslate_roles": [json.dumps(["platform-admin"] if name == "admin" else [])]},
                      "credentials": [{"type": "password", "value": passwords[name], "temporary": False}]})
    realm = {"realm": "dataslate", "enabled": True, "sslRequired": "all", "registrationAllowed": False,
             "accessTokenLifespan": 120, "ssoSessionIdleTimeout": 600,
             "clients": [{"clientId": "dataslate-web", "protocol": "openid-connect", "publicClient": True,
                          "standardFlowEnabled": True, "directAccessGrantsEnabled": False,
                          "implicitFlowEnabled": False, "serviceAccountsEnabled": False,
                          "redirectUris": [APP + "/auth/callback"], "webOrigins": [APP],
                          "attributes": {"pkce.code.challenge.method": "S256"},
                          "protocolMappers": mappers}], "users": users}
    secret("keycloak-realm", {"dataslate-realm.json": json.dumps(realm)})
    write("realm.json", json.dumps(realm))
    items.append(obj("PersistentVolumeClaim", "postgres-data", spec={"accessModes": ["ReadWriteOnce"],
                     "storageClassName": "local-path", "resources": {"requests": {"storage": "2Gi"}}}))
    security = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"]}}
    base = {"automountServiceAccountToken": False,
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}}}
    pg = base | {"securityContext": base["securityContext"] | {"runAsUser": 999, "runAsGroup": 999, "fsGroup": 999},
                 "containers": [{"name": "postgres", "image": IMAGES["postgres"], "securityContext": security,
                  "args": ["postgres", "-c", "ssl=on", "-c", "ssl_cert_file=/tls/tls.crt", "-c", "ssl_key_file=/tls/tls.key"],
                  "env": [{"name": "POSTGRES_PASSWORD_FILE", "value": "/init/admin-password"},
                          {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"}],
                  "ports": [{"containerPort": 5432}],
                  "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "384Mi"}},
                  "readinessProbe": {"exec": {"command": ["pg_isready", "-U", "postgres"]}, "periodSeconds": 3},
                  "volumeMounts": [{"name": "data", "mountPath": "/var/lib/postgresql/data"},
                      {"name": "tls", "mountPath": "/tls", "readOnly": True},
                      {"name": "init", "mountPath": "/init", "readOnly": True},
                      {"name": "init", "mountPath": "/docker-entrypoint-initdb.d", "readOnly": True},
                      {"name": "run", "mountPath": "/var/run/postgresql"}, {"name": "tmp", "mountPath": "/tmp"}]}],
                 "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "postgres-data"}},
                      {"name": "tls", "secret": {"secretName": "fixture-tls", "defaultMode": 0o440}},
                      {"name": "init", "secret": {"secretName": "postgres-init", "defaultMode": 0o440}},
                      {"name": "run", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]}
    kc = base | {"securityContext": base["securityContext"] | {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
                 # The disposable upstream IdP performs its own first-start build.
                 # This does not change DataSlate's read-only production containers.
                 "containers": [{"name": "keycloak", "image": IMAGES["keycloak"], "securityContext": security | {"readOnlyRootFilesystem": False},
                  "args": ["start", "--import-realm", "--db=postgres", "--hostname=" + ISSUER.split("/realms/")[0],
                           "--https-certificate-file=/tls/tls.crt", "--https-certificate-key-file=/tls/tls.key",
                           "--http-enabled=false", "--cache=local", "--health-enabled=true"],
                  "env": [{"name": "KC_DB_URL", "value": f"jdbc:postgresql://{host}:5432/keycloak?sslmode=verify-full&sslrootcert=/tls/ca.crt"},
                          {"name": "KC_DB_USERNAME", "value": "dataslate_idp"},
                          {"name": "KC_DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "keycloak-db", "key": "password"}}}],
                  "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "2", "memory": "1200Mi"}},
                  "ports": [{"containerPort": 8443}],
                  "readinessProbe": {"httpGet": {"scheme": "HTTPS", "path": "/realms/dataslate", "port": 8443}, "periodSeconds": 5, "initialDelaySeconds": 20},
                  "volumeMounts": [{"name": "tls", "mountPath": "/tls", "readOnly": True},
                      {"name": "realm", "mountPath": "/opt/keycloak/data/import", "readOnly": True},
                      {"name": "data", "mountPath": "/opt/keycloak/data"}, {"name": "tmp", "mountPath": "/tmp"}]}],
                 "volumes": [{"name": "tls", "secret": {"secretName": "fixture-tls", "defaultMode": 0o440}},
                      {"name": "realm", "secret": {"secretName": "keycloak-realm", "defaultMode": 0o440}},
                      {"name": "data", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]}
    for name, pod, port, target in [("postgres", pg, 5432, 5432), ("keycloak", kc, 18443, 8443)]:
        items.append(obj("Deployment", name, spec={"replicas": 1, "strategy": {"type": "Recreate"},
                     "selector": {"matchLabels": {"fixture": name}},
                     "template": {"metadata": {"labels": {"fixture": name}}, "spec": pod}}))
        items.append(obj("Service", name, spec={"selector": {"fixture": name}, "ports": [{"port": port, "targetPort": target}]}))
    write("fixtures.json", json.dumps({"apiVersion": "v1", "kind": "List", "items": items}))
    write("fixture-info.json", json.dumps({"namespace": NS, "applicationNamespace": APP_NS,
          "issuer": ISSUER, "applicationOrigin": APP, "images": IMAGES}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New private directory; must not exist")
    args = parser.parse_args()
    generate(args.output)
    print("Private disposable fixtures generated; no cluster was contacted.")

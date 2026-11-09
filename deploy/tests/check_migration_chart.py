"""Prove separate migrator credentials never reach long-running workloads."""
import yaml


def check_migration_chart(run, external_args):
    args = [*external_args, "--set", "migrations.databaseSecret.name=dataslate-migrator",
            "--set", "migrations.databaseSecret.key=owner-uri"]
    docs = [d for d in yaml.safe_load_all(run(args)) if d]
    job = next(d for d in docs if d["kind"] == "Job")
    pod = job["spec"]["template"]["spec"]
    env = {v["name"]: v for v in pod["containers"][0]["env"]}
    assert env["DATASLATE_DATABASE_URL"]["valueFrom"]["secretKeyRef"] == {"name": "dataslate-migrator", "key": "owner-uri"}
    assert env["PGSSLMODE"]["value"] == "verify-full"
    assert env["PGSSLROOTCERT"]["value"] == "/etc/dataslate/postgres/ca.crt"
    assert "dataslate-database-ca" in str(pod["volumes"])
    for doc in docs:
        if doc["kind"] == "Deployment":
            assert "dataslate-migrator" not in str(doc), "Migration credential leaked into runtime deployment"
    for override in ["migrations.databaseSecret.name=owner-only", "migrations.databaseSecret.key=key-only",
                     "migrations.databaseSecret.name=INVALID", "migrations.databaseSecret.key=bad/key",
                     "migrations.databaseSecret.password=forbidden"]:
        run([*external_args, "--set", override], "databaseSecret")
    print("PASS separate migration credential: Job-only owner, shared verified CA, 5 invalid references rejected")

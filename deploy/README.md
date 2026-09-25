# DataSlate Kubernetes foundation

This directory deploys the **real DataSlate API and web application**, with verified OIDC, PostgreSQL persistence and a versioned migration Job. It supports an existing PostgreSQL database or a persistent CloudNativePG `Cluster`. The first target is **AWS EKS with S3**, serving teams within one organization.

**This is an implementation foundation, not a completed lakehouse distribution.** The UI/API currently edit workspace resources and generate deployment plans and Spark manifest previews. They do not run Spark, SQL, ingestion or Sylon requests. See [the capability inventory](capabilities.md) for every broader module and its current status. Installing this chart does not install those modules.

## Ownership model

| Resource | Owner |
|---|---|
| API/web Deployments, Services, network policies, PDBs, migration Job | This DataSlate Helm release |
| Existing PostgreSQL, IdP, ingress controller, registry | Customer; referenced, never adopted or upgraded |
| Optional managed PostgreSQL `Cluster` | DataSlate chart owns the CR; the explicitly selected CNPG operator owns database Pods, generated Secrets and PVCs |
| CNPG or Spark operator, their CRDs and cluster RBAC | A **separate administrator-owned release**, installed only after checking for an existing compatible installation |
| S3 lake, IAM roles, EKS add-ons, storage classes, DNS/TLS | Administrator/cloud infrastructure; no automatic mutation from the application |

The application service accounts have **no Kubernetes API roles and no mounted service-account tokens**. Preview generation needs no cluster credentials. A future submitter will receive narrowly scoped execution privileges after admission, workspace identity and cancellation/reconciliation tests exist. Never give the API cluster-admin to enable a planned module.

## Prerequisites

1. A dedicated namespace and a CNI enforcing Kubernetes NetworkPolicy. The [namespace example](examples/namespace.yaml) enables restricted Pod Security for a new namespace; inspect existing labels before applying it.
2. Built and published DataSlate API/web images, using each component's Dockerfile. The example registry URLs are placeholders, not published product images. Use immutable image digests for release deployments.
3. An OIDC provider configured for a public browser client, Authorization Code + PKCE, an access token for `dataslate-api`, and the exact HTTPS callback `https://YOUR_HOST/auth/callback`. The browser client needs no client secret. Map the claims described in [the foundation contract](../docs/implementation/foundation-contract.md); do not grant every authenticated user platform administrator.
4. A reachable PostgreSQL database with a dedicated `dataslate` database owner for initial migration, and trusted TLS. Supply a Secret containing `database-url` and a separate Secret containing the trusted `ca.crt` bundle. Do not put credentials in Helm values, Git, terminal history or examples. The URL must use the database's real DNS name and `sslmode=verify-full`. The chart also sets `PGSSLMODE=verify-full` and `PGSSLROOTCERT`; the API must reject insecure production settings.
5. For the managed database profile: a reviewed CNPG operator, three schedulable nodes, an explicit persistent storage class, and a backup/restore design. The supplied cluster requires distinct hosts, not distinct availability zones; qualify/configure zone placement before promising AZ resilience. PVCs use the class's storage policy. The [EBS class example](examples/eks-storageclass.yaml) uses encrypted gp3 and retains volumes.
6. An existing ingress controller and TLS configuration. The EKS example uses an **internal ALB**, IP targets, ACM termination and a TLS 1.2/1.3 listener policy. Replace the sample ARN, subnets and hostname. NetworkPolicy permits only the configured ALB subnet peers to the web service; AWS security groups and actual VPC CNI behavior also require verification. [AWS controller annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/ingress/annotations/).

EBS volumes are block storage for PostgreSQL. S3 is the object-storage data lake. The initial API does not read or write S3, so these Pods receive no S3 role. Future Spark driver/executor and catalog identities must be separately scoped; associating a role with a service account does not prove that every selected connector/runtime supports its credential chain. See [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) and [EBS CSI](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html). EKS Auto Mode uses a different EBS provisioner; do not apply the standard EKS StorageClass unchanged there.

## Validate without a cluster

From the repository root:

```sh
python3 -m venv deploy/.cache/venv
deploy/.cache/venv/bin/python -m pip install -r deploy/tests/requirements.txt
python3 deploy/scripts/fetch-helm.py
HELM_BIN="$PWD/deploy/.cache/helm" PYTHON_BIN="$PWD/deploy/.cache/venv/bin/python" deploy/scripts/check.sh
```

The helper downloads **Helm 4.3.0** only into ignored `deploy/.cache`, checking a reviewed SHA-256. It neither installs a system package nor touches kubeconfig. Supported helper targets are macOS arm64 and Linux amd64; use a separately verified Helm on other platforms. The checksum validates the pinned bytes; maintainer signature verification and artifact provenance remain part of release supply-chain policy. [Helm installation and verification](https://helm.sh/docs/intro/install/).

Checks invoke real Helm lint/template, parse the rendered resources, verify identity/TLS/least privilege/persistence, and assert rejection of invalid settings. Helm lint cannot inject custom API versions, so the **managed profile is validated by actual template rendering with the CNPG API capability**, while lint runs the external profile. Optional upstream chart lint/render runs if `python3 deploy/scripts/fetch-upstream.py` has fetched the checksum-locked archives first. No check contacts Kubernetes.

To inspect a manifest yourself:

```sh
deploy/.cache/helm template dataslate deploy/helm/dataslate \
  --namespace dataslate --kube-version 1.34.0 \
  -f deploy/examples/eks-external.yaml

deploy/.cache/helm template dataslate deploy/helm/dataslate \
  --namespace dataslate --kube-version 1.34.0 \
  --api-versions postgresql.cnpg.io/v1/Cluster \
  -f deploy/examples/eks-external.yaml \
  -f deploy/examples/eks-managed-postgres.yaml
```

Default values deliberately fail without identity, database and network configuration. Helm has no development/anonymous/in-memory authentication option. The application core needs Kubernetes 1.29+ APIs; the pinned **Apache Spark operator separately requires Kubernetes 1.34+ and Spark 4.0+**. The first combined qualification target is 1.34, subject to the EKS versions available in the customer's region. [Apache operator requirements](https://apache.github.io/spark-kubernetes-operator/).

## Install after infrastructure review

These commands are instructions, **not actions performed by this implementation**. Create a private, reviewed values file from the external example; change all image, IdP, database, network and TLS references. For a managed database, add a private overlay equivalent to the managed example only after following [operator bootstrap](bootstrap/README.md).

```sh
helm upgrade --install dataslate deploy/helm/dataslate \
  --namespace dataslate \
  --values /secure/config/dataslate-values.yaml \
  --wait --wait-for-jobs --timeout 15m
```

The migration Job is a **normal release resource**, not a pre-install hook: an in-cluster database must be created before migration can connect. API readiness requires the migrated schema; liveness is separate. Migration failure leaves the API unready and inspectable. Increasing the install timeout alone cannot fix a missing IdP, image, CA, network route or incompatible schema.

For controlled upgrades, render/review migration changes, test them against a restored backup, and use `migrations.enabled=false` only when an administrator has already run the exact image's `/dataslate-api migrate` with the same connection/TLS settings. Concurrent migration Jobs are serialized by the API's database advisory lock. Schema changes need forward/backward compatibility; a Helm rollback does **not** undo a database migration.

## Verification and operational limits

After installation:

```sh
kubectl -n dataslate rollout status deployment/dataslate-api
kubectl -n dataslate rollout status deployment/dataslate-web
kubectl -n dataslate get jobs,pods,pdb,networkpolicy
python3 deploy/scripts/verify-install.py --url https://YOUR_HOST
```

The read-only HTTP verifier checks static web health, API readiness, OIDC configuration and rejection of anonymous API access. It does not prove a real login or cross-workspace authorization. Complete those tests with separate test identities. Test changing/expired credentials, database failover, resource limits, network restrictions and rolling upgrades on the actual cluster.

NetworkPolicy restricts application ingress and egress. The default API HTTPS egress permits TCP 443 to IPv4 destinations for OIDC discovery/JWKS; restrict it to approved gateways/IdP networks where stable. Set `networkPolicy.httpsEgressPorts` to the approved HTTPS ports used by the issuer and its discovery/JWKS endpoints. For example, `oidc.issuer: https://identity.example.com:8443/realms/dataslate` needs `httpsEgressPorts: [8443]`; include another port only if a required HTTPS endpoint actually uses it. This changes network access, not TLS verification or the configured issuer URL.

Set `networkPolicy.postgresPort` to the port in the **external** database URL Secret (default `5432`); a database URL using `:6543` needs `postgresPort: 6543`. This value does not rewrite the Secret or reconfigure the database. Managed CloudNativePG continues to use `5432`, and the chart rejects custom PostgreSQL ports for that profile. Both port settings accept integers from 1 through 65535, and the HTTPS port list must be nonempty and unique.

Standard NetworkPolicy cannot express DNS-name allowlists. Adjust DNS selectors for your cluster; NodeLocal DNSCache and IPv6 need explicit review. The chart does not install the CNI or change an existing namespace's network policies. PostgreSQL's operator-specific network policy and control-plane ports remain separately owned and must be qualified.

External Secret changes do not restart Pods that consume environment variables. Roll the API and rerun relevant checks after database credential changes. CA mounts update as secret volumes, but existing client connections/caches still require a tested rotation procedure. Never print the database URL when debugging migration failures.

Two API/web replicas and PDBs support planned disruption; they do not establish availability or performance SLAs. Scheduling, load tests and database failure testing remain outstanding. CPU/memory settings are initial requests, not sizing claims.

The managed CNPG `Cluster` defaults to Helm's **keep** policy. Uninstalling the app intentionally retains that database resource; it then needs explicit administration or deliberate adoption by a future release. Removing the CNPG operator or its CRDs has separate and potentially destructive consequences. A retained volume is not a backup. **Managed PostgreSQL is not production qualified until scheduled WAL/base backups and a restore drill are implemented and verified**; the Barman plugin integration is a planned step, not silently configured here.

## First EKS qualification gate

- Build, scan and start both actual images under restricted Pod Security/read-only filesystems.
- Complete OIDC PKCE login, API audience validation, expiration and cross-team access tests.
- Complete PostgreSQL TLS validation, migration/save/reopen, interrupted migrations, failover and restore.
- Validate internal ALB/DNS/TLS and actual NetworkPolicy enforcement; verify anonymous access is denied.
- Inventory existing operators/CRDs, then verify chosen versions and ownership before bootstrap.
- Before execution: submit/reconcile/cancel real Spark jobs, establish workspace admission and service-account restrictions, test driver/executor S3 credentials and cloud commit behavior.
- Before governed SQL: prove catalog/user identity, row filters, column masking and direct-file-access boundaries together.

Passing static rendering does not satisfy these live gates.

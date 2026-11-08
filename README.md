<p align="center">
  <img src="brand/github-banner.png" alt="DataSlate — Your lake. Your workspace. An open data and AI workspace for Kubernetes. Foundation build." width="100%" />
</p>

<p align="center">
  <a href="https://nimeshbuilds.github.io/dataslate/">Explore DataSlate</a> ·
  <a href="deploy/README.md">Deployment</a> ·
  <a href="docs/implementation/coverage.md">Roadmap</a> ·
  <a href="CONTRIBUTING.md">Contribute</a>
</p>

<p align="center">
  <a href="https://github.com/NimeshBuilds/dataslate/actions/workflows/ci.yml"><img src="https://github.com/NimeshBuilds/dataslate/actions/workflows/ci.yml/badge.svg" alt="Foundation checks" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-123D32" alt="Apache-2.0 license" /></a>
  <img src="https://img.shields.io/badge/status-foundation-DDE9DE" alt="Status: foundation" />
</p>

# DataSlate

**Your lake. Your workspace.** An open-source Kubernetes platform for data engineering, analytics, machine learning and agents, with **Sylon** as its integrated assistant.

DataSlate is being built as an open-source product with paid support/hosting as a distribution option. It targets teams within one organization, starting with **AWS EKS and S3**. It will support both explicitly managed dependencies and compatible existing services. The complete scope includes Spark, lakehouse governance, ingestion, workflows, notebooks, SQL, visualization, ML, serving, feature stores, agents, memory, apps and sharing.

**Current maturity: implementation foundation.** Source editing/persistence, authorization and planning are implemented. An opt-in controller implements approved immutable Spark applications and saved Python notebook batches with durable run state. Notebook batches snapshot an exact saved revision and require an explicit engineering execution grant. Local operator tests and outstanding EKS/S3 qualification are recorded separately. Interactive sessions, SQL, rich results and AI execution remain under development. No query or assistant result is fabricated. This is not a production-qualified Databricks replacement yet.

## A familiar workspace, on infrastructure you control

DataSlate brings notebooks, SQL, catalogs, compute and AI into one product experience. The architecture connects mature open-source services through a shared identity, policy and resource model. Teams can use compatible existing services or explicitly provision managed dependencies.

| Available in the foundation | Integration roadmap |
|---|---|
| Python/SQL source editors, revision conflict handling and immutable Python notebook batch snapshots | Interactive kernels, SQL sessions, result tables, charts and profiling |
| Verified OIDC access tokens and workspace roles | Catalog governance, consistent row/column enforcement and lineage |
| PostgreSQL persistence and managed/external dependency planning | S3/ADLS/GCS lake connections, Delta/Iceberg, ingestion and maintenance |
| Apache/Kubeflow previews; opt-in Apache Spark controller, run UI/SDK, source provenance and recovery | Cloud runtime qualification, workflow scheduling, streaming and operational compute controls |
| Kubernetes Helm deployment scaffolding and Python SDK/CLI | Sylon gateways, agents, memory, ML, serving, apps and sharing |

Each integration has its own validation gate. See the [capability inventory](docs/implementation/coverage.md) for the complete scope and the [validation record](docs/implementation/validation.md) for evidence.

## Repository

| Path | Responsibility |
|---|---|
| `api/` | Go control plane, PostgreSQL storage/migrations, OIDC authorization, module planner, Spark manifest compiler and durable batch controller |
| `web/` | React/TypeScript workspace, notebook/SQL source editor, batch run lifecycle and installation/capability views |
| `sdk/python/` | Python SDK and `dataslate` CLI sharing the HTTP API |
| `deploy/` | Helm, managed/external PostgreSQL configuration, EKS guidance and explicit upstream bootstrap |
| `examples/` | API inputs; previews do not execute them |
| `docs/implementation/` | Shared contracts, full coverage and validation status |
| `docs/architecture/` | Current decisions and source-linked research snapshots |

## Run the application for development

This loopback workflow develops the Kubernetes product; it is not a separate local-compute edition. Requires Go matching `api/go.mod`, Node 22.19+ and npm. Use separate terminals:

```sh
make dev-api
```

```sh
cd web
npm ci
npm run dev
```

Open the Vite URL printed in the second terminal. The API listens on `127.0.0.1:8080` and the development frontend proxies API requests. Explicit development mode uses a marked development administrator and in-memory storage, which resets when the API restarts. It refuses non-loopback API listening. Production uses verified OIDC and PostgreSQL; the Helm chart cannot select development mode.

For CLI/SDK development with Python 3.11+:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install ./sdk/python
.venv/bin/dataslate status
.venv/bin/dataslate workspace list
.venv/bin/dataslate notebook create --workspace default --file examples/notebook.json
.venv/bin/dataslate spark preview --workspace default --file examples/spark-preview.json
```

See [CLI authentication and commands](cli/README.md), [API documentation](api/README.md) and [deployment documentation](deploy/README.md). Kubernetes installation requires built images, configured OIDC, storage, credentials by reference and compatible operator versions. No cloud resources are provisioned by running the development commands.

## Build and validate

```sh
make test-api
make test-sdk PYTHON=python3
make check-web
```

Browser tests and Helm checks are described in their component directories. The CI workflow covers automated checks and isolated PostgreSQL where available; a CI configuration is not evidence that a remote run has occurred. [Validation status](docs/implementation/validation.md) distinguishes local checks from pending live-cloud tests.

`make test-local` combines the source checks, PostgreSQL integration, browser tests and deployment validation after their prerequisites are installed. It requires an isolated `DATASLATE_TEST_DATABASE_URL` and does not provision infrastructure. The [first Spark notebook example](examples/python-batch-notebook.json) and [CLI execution guide](cli/README.md) exercise a deterministic aggregation through a configured engineering binding.

## Product scope and engineering gates

The [complete capability inventory](docs/implementation/coverage.md) is the implementation backlog, including broader ML/AI/app/sharing features. [Foundation contracts](docs/implementation/foundation-contract.md) define the first shared API. [Architecture decisions](docs/architecture/README.md) explain the intended complete system.

The critical first integration is UC OSS + Delta + Kyuubi/Spark SQL/Ranger with coherent identities and cloud credentials. Independent upstream version support is not proof of combined row/column enforcement. Result caches, notebook outputs, agents and memory must preserve the same restrictions. Managed/external installation must respect ownership and CRD lifecycle. These gates remain release requirements rather than marketing claims.

## License

Original DataSlate code is under [Apache-2.0](LICENSE). Separately distributed dependencies retain their own licenses and [notices](NOTICE). Brand source files and usage guidance are in [brand/](brand/README.md).

DataSlate is an independent project maintained under [NimeshBuilds](https://github.com/NimeshBuilds). It is not affiliated with or endorsed by Databricks or the upstream projects mentioned here. Their names identify interoperability targets, not certification. Hosted services and published runtime packages are not available yet.

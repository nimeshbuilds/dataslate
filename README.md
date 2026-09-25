# DataSlate

**One Kubernetes workspace for data engineering, analytics, machine learning and agents.** Sylon is the integrated assistant.

DataSlate is being built as an open-source product with paid support/hosting as a distribution option. It targets teams within one organization, starting with **AWS EKS and S3**. It will support both explicitly managed dependencies and compatible existing services. The complete scope includes Spark, lakehouse governance, ingestion, workflows, notebooks, SQL, visualization, ML, serving, feature stores, agents, memory, apps and sharing.

**Current maturity: implementation foundation.** This repository begins the real application and deployment code. Source editing/persistence, authorization, dependency planning and Spark manifest previews are separate from live Spark/SQL/AI execution. Planned capabilities are shown honestly; no query or assistant result is fabricated. This is not a production-qualified Databricks replacement yet.

## Repository

| Path | Responsibility |
|---|---|
| `api/` | Go control plane, PostgreSQL storage/migrations, OIDC authorization, module planner and Spark manifest compiler |
| `web/` | React/TypeScript workspace, notebook/SQL source editor and installation/capability views |
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

## Product scope and engineering gates

The [complete capability inventory](docs/implementation/coverage.md) is the implementation backlog, including broader ML/AI/app/sharing features. [Foundation contracts](docs/implementation/foundation-contract.md) define the first shared API. [Architecture decisions](docs/architecture/README.md) explain the intended complete system.

The critical first integration is UC OSS + Delta + Kyuubi/Spark SQL/Ranger with coherent identities and cloud credentials. Independent upstream version support is not proof of combined row/column enforcement. Result caches, notebook outputs, agents and memory must preserve the same restrictions. Managed/external installation must respect ownership and CRD lifecycle. These gates remain release requirements rather than marketing claims.

## License

Original DataSlate code is under [Apache-2.0](LICENSE). Separately distributed dependencies retain their own licenses. No upstream project's name or license establishes compatibility with DataSlate. No GitHub organization, hosted service or public package has been registered by this initial repository work.

# DataSlate foundation contract

Product: **DataSlate**. Assistant: **Sylon**. Initial validation target: AWS EKS and customer-owned S3. Deployment serves teams in one organization. Distribution: open source with paid support/hosting. Broader data, BI, ML, agents, serving, apps, sharing and operations are committed product scope; status must distinguish implemented code, tested adapters and production-qualified integration.

This contract records the first implementation. Later [batch execution](execution-contract.md), [ingestion](ingestion-contract.md), [SQL](sql-contract.md), [catalog](catalog-contract.md), [search](search-contract.md) and [Sylon](sylon-contract.md) contracts extend it; their precise definitions take precedence for those resources. Never simulate a successful query, Spark submission, connection verification or assistant response. Unsupported execution returns a typed capability error and the UI explains the missing integration. Approved immutable applications and explicitly granted saved Python notebook batches use the durable API and Apache operator adapter. Sylon uses a separate durable generation controller and reviewed code application. Interactive notebook sessions and agent tool execution remain separate integrations. Live EKS/S3 qualification is a separate release gate. API generation/validation is not execution.

## Repository ownership

- `api/`: Go control-plane service, SQL migrations, API tests, OpenAPI and API Dockerfile.
- `web/`: React/TypeScript workspace, browser tests, frontend Dockerfile.
- `deploy/`: Kubernetes/Helm, example values, installation verification and CI for charts.
- `sdk/python/`, `cli/`, root docs/CI/scripts: root integration work.

## Shared transport

JSON REST under `/api/v1`, same-origin browser traffic; errors use `{ "error": { "code": "...", "message": "..." } }`. Lists use `{ "items": [...] }`. Reject unknown/untrusted fields for mutations, bound bodies, validate IDs and optimistic revision updates. Every resource includes `id`, `workspaceId` where relevant, `createdAt`, `updatedAt`, and integer `revision`. Display names are not identity.

Public endpoints:

- `GET /healthz`, `GET /readyz`.
- `GET /api/v1/auth/config` -> `{mode, issuer?, clientId?, audience?, scopes?}`. Public identifiers only, never credentials.

Authenticated endpoints:

- `GET /api/v1/status` -> `{product:"DataSlate", assistant:"Sylon", phase:"foundation", authMode, persistence:"memory"|"postgres", executionEnabled:boolean, executionMode:"approved-batch"|"disabled", notebookExecutionEnabled:boolean}`. Configured bindings determine these flags; notebook execution means saved Python batch capability. They do not assert backend health or grant the caller execution access.
- `GET /api/v1/workspaces`; `POST /api/v1/workspaces` body `{name, slug}`; create requires platform administrator.
- `GET /api/v1/workspaces/{workspaceId}/notebooks`; `POST` body `{name, kind:"notebook"|"sql", cells:[{id,language:"python"|"sql",source}]}`.
- `GET /api/v1/workspaces/{workspaceId}/notebooks/{id}`; `PUT` body `{name,kind,cells,revision}`. Editor/admin only for writes. No execution outputs in shared notebook documents.
- `GET /api/v1/modules` -> `{items:[{id,name,category,implementation:"foundation"|"planned",description,dependencies:[string],modes:["managed","external"],enabledByDefault:boolean}]}`.
- `POST /api/v1/deployment-plans` body `{namespace, cloud:"aws", modules:[{id,mode:"managed"|"external",endpoint?}]}` -> `{cloud,namespace,actions:[{moduleId,mode,action,verification:"required"}],warnings:[string],executable:false}`. Validate required dependencies/cycles and HTTPS endpoint requirements. No network probe or install side effect. Default application core is implicit; optional modules are explicit.
- `POST /api/v1/workspaces/{workspaceId}/spark-applications/preview` body `{name,operator:"apache"|"kubeflow",image,mainApplicationFile,serviceAccount,namespace,executorInstances}` -> `{manifest:object,warnings:[string],submitted:false}`. JSON Kubernetes manifest for the precise CRD API. Preview only. Image/URI/name/namespace validation; no arbitrary pod fields. Authenticated workspace editor required. The output must explain runtime compatibility and admission/identity checks remain necessary before submission.

Approved batch bindings, runs, events, cancellation and driver logs use the same workspace routes and owner-scoped authorization documented in [the execution contract](execution-contract.md). Their exact transport schemas are in OpenAPI.

The backend owns `api/openapi.yaml` and documents any precise model additions; notify the frontend/root of contract changes.

## Identity and persistence

Production: OIDC JWT verification with configured issuer and audience, validated signature and expiration, no fallback to anonymous/admin; PostgreSQL required. An access token must be minted for the API audience; an ID token is not automatically an API credential. Server metadata carries no secrets.

Initial claim contract: `sub` identifies the caller; `dataslate_roles` may contain `platform-admin`; `dataslate_workspaces` is an object mapping workspace IDs to `viewer`, `editor`, or `admin`. Deny absent membership. Apply workspace authorization in every resource query and mutation. Use established OIDC/JWKS libraries. Never trust forwarded user/group headers.

Explicit loopback development mode may use in-memory storage and a clearly marked development admin. It must never be the Helm deployment default. Development mode must refuse non-loopback listening. Production readiness requires storage availability. Migrations are explicit and versioned.

Environment contract: `DATASLATE_ADDR` (loopback default), `DATASLATE_AUTH_MODE` (`oidc` default or explicit `development`), `DATASLATE_DATABASE_URL`, `DATASLATE_OIDC_ISSUER`, `DATASLATE_OIDC_AUDIENCE`, `DATASLATE_OIDC_CLIENT_ID`, `DATASLATE_OIDC_SCOPES` (space-separated). Document optional configuration. No production secret in examples. Browser should use Authorization Code + PKCE via a maintained OIDC client, and access token for API; callback `/auth/callback`. Avoid token logging or persistent localStorage tokens.

## Module identities

Use these stable IDs consistently: `postgres`, `spark-operator`, `spark`, `unity-catalog`, `delta`, `kyuubi`, `trino`, `polaris`, `ranger`, `zookeeper`, `airflow`, `ingestion`, `kafka`, `cdc`, `pipelines`, `maintenance`, `lineage`, `dashboards`, `mlflow`, `sylon`, `agents`, `memory`, `headroom`, `model-serving`, `feature-store`, `search`, `apps`, `sharing`, `iceberg`, `federation`, `observability`, `secrets`, `identity`, `registry`. Their dependency graph and integration status are engineering metadata, not a claim they are installed. Application configuration and resource ownership use `managed` or `external`; enablement is separate.

## Validation

API: authorization matrix, cross-workspace ID access, stale revisions, invalid manifests, no false execution, dependency validation, production configuration fail-closed; test real handlers, not implementation mirrors. Approved batch execution additionally requires idempotency, concurrent capacity enforcement, durable leases/fencing, grant revocation, ambiguous submission recovery, ownership-verified logs and cancellation tests. UI: save/reopen a notebook, API error and offline states, planned capability disclosure, no simulated query/AI results. Helm: lint/render production and managed/external examples; resource requests, security contexts, probes, persistent state, secrets by reference. Live cloud/OIDC/operator tests remain explicit gates until infrastructure exists.

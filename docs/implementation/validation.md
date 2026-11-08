# Implementation validation

This records implementation evidence, not production certification. The local development application uses explicit loopback authentication and memory storage. The production code path requires OIDC and PostgreSQL.

## Sylon and combined integration checkpoint — November 7–8, 2026

This checkpoint adds private Sylon conversations, authorized code/catalog/search context and reviewed cell changes to the ingestion, SQL, catalog and search foundations below. It does not close the full-platform roadmap. Local evidence is distinct from external provider, cloud and complete installation qualification.

| Check | Observed result and boundary |
|---|---|
| Sylon backend | Twenty-two Sylon top-level tests and their subcases passed with memory and PostgreSQL stores. The complete API race suite passed with pgvector mandatory, followed by vet. A real local HTTPS Responses fixture exercised provider transport, persisted generation and explicit cell application. No paid model API was used. See [the exact contract](sylon-contract.md). |
| Race and authorization fixes | Selected-cell revision is checked inside the publication transaction; changes during generation discard the response. Lease expiration is evaluated after acquiring the row lock. Derived history rechecks every supplied source, including uncited sources; deleted or revoked dependencies deny access. Concurrent apply stores one durable receipt and never re-executes code. The Sylon conversation/job relationship has a composite workspace foreign key. |
| Provider adapters | Fifteen LLM and eleven embeddings top-level tests plus adversarial TLS/OAuth subcases passed. Generation uses a bounded Responses or Chat Completions contract, exact model identity and one attempt. Unknown dispatch is never automatically repeated. Consumer subscription login, streaming and individual native provider profiles are not claimed. |
| SDK/CLI and real API smoke | All 56 SDK/CLI tests and 13 real development API smoke groups passed, including current route/receipt alignment, exact selection, idempotency, unknown outcome, owner scope, explicit apply and unconfigured Sylon refusal. The smoke launcher clears inherited backend binding files. |
| Native production controller processes | SQL, search and Sylon release binaries passed startup, multiple idle reconciliation ticks, readiness, restart and graceful shutdown against PostgreSQL 17.11 with verified TLS and a restricted runtime role. Every executable rejected disabled TLS. The runtime role could neither create schema objects nor mutate migration records. See [sanitized evidence](../../deploy/control-plane-qualification.json) and [the repeatable helper](../../scripts/qualify-control-plane.py). These were native processes, not an EKS deployment or live-job test. |
| Final workspace | TypeScript/Vite production build and all 81 browser tests passed, including 16 Sylon tests. Cases cover private conversation recovery, saved-source/revision conflicts, explicit one-use apply, viewer restrictions, revoked outputs, uncertain dispatch and scoped context. Desktop/narrow layouts were reviewed. Browser model fixtures are controlled test responses; real TLS adapter tests are separate. |
| Final Helm | The combined chart suite passed, including base 18, Spark 27, SQL 37, Catalog 36, Search 40 and Sylon 41 unsafe-configuration rejection cases. Provider secrets are worker-only; indirect search/catalog dependencies mount only selected credentials and receive explicit egress. Both database modes render. This is render/policy validation, not a complete cluster installation. |
| Final OCI builds and startup | All six Linux ARM64 images built and passed startup checks by immutable manifest/config digest: API, execution controller, SQL controller, search controller, Sylon controller and web. Processes ran non-root with read-only roots, zero effective capabilities and no-new-privileges. All four controllers completed two startup/reconciliation/shutdown cycles against verified PostgreSQL TLS and rejected insecure DB configuration with the expected application error. API/web checks covered proxy/static assets, headers, callback-log redaction and invalid configuration. Spark discovery used a TLS protocol fixture; this is not a new live-operator job test. See [exact image evidence](../../deploy/container-qualification.json) and [reproduction instructions](../../deploy/container-qualification.md). |
| Runtime and profile checks | Twenty-three ingestion contract tests, eight Trino profile tests, five Polaris profile tests and ten compute-boundary tests passed again against the final source checkpoint. The real JVM, distributed SQL and Polaris checks below remain separate evidence. |
| Product site | Updated feature/roadmap text passed browser checks at 1440, 390 and 320 pixels: no horizontal overflow, missing resources or page errors; tabs, navigation and internal links passed. Desktop/mobile layouts were visually inspected. Sample product illustrations remain explicitly labeled. |

The complete local runner now requires pgvector tests instead of silently skipping hybrid retrieval. Application OCI bases are immutable and checked against the release lock. Both qualification helpers explicitly refuse optimized Python execution; negative startup checks require the expected application error rather than any process failure. Staged API production sources and all six Dockerfiles matched their qualified snapshots; UI source matched the browser snapshot with only the documented Vite development-harness overrides. A targeted scan of 236 staged text files found no private keys, AWS key identifiers, GitHub tokens, fixture database secrets or private host paths. This limited scan is not a comprehensive security audit. The current images have not been published as a registry release. Local checks do not establish a qualified deployment or comparative performance claim.

## Ingestion, SQL, Polaris and search checkpoint — November 7, 2026

This earlier local checkpoint extends the saved-notebook implementation below. The subsequent Sylon checkpoint above supersedes its client and workspace totals. Tests qualify each stated boundary; they do not prove the complete cloud workflow.

| Check | Observed result and boundary |
|---|---|
| Incremental ingestion runtime | Twenty-three contract tests and fourteen real Spark/Delta scenarios passed on the host JVM. The same fourteen scenarios passed inside the production Linux ARM64 OCI image as UID 185, with networking disabled, 4 GiB memory and two CPUs. Coverage includes JSON/Parquet schema enforcement, Snappy publication, checkpoint restart, duplicate batch recovery and malformed-source failure. See [runtime evidence](../../runtime/ingestion/qualification.json). Local files do not establish S3/IRSA behavior. |
| Durable control plane | Full Go race suite and vet passed with real PostgreSQL 17.11 and required public pgvector 0.8.7. Tests cover ingestion storage/writer ownership, SQL submission/result/cancellation, exact catalog grants, index capacity, revocation, source deletion, current revision filtering, lost provider responses and leased recovery. Gated live backend tests are distinct from routine unit fixtures. |
| Governed SQL | Real Trino 484 coordinator and worker reached Ready using the pinned managed TLS/JWT profile. The production Go transport returned five filtered/masked rows and preserved exact bigint/decimal values. Mutation, ungranted tables, system metadata, runtime-limit overrides and unauthorized impersonation were denied; cancellation reached an observed terminal state. NetworkPolicy denied an unlabeled Pod, allowed the approved label, then denied it after removal. See [SQL qualification](../../deploy/sql/qualification.json). This used TPCH rather than S3/Delta/Iceberg. |
| Polaris | Pinned Polaris 1.8.0/JDBC bootstrap and production readiness passed with TLS. A separate explicitly insecure FILE fixture created native Iceberg v2 metadata; real discovery/list/load and two-field schema access passed, along with missing-auth and network-policy denial. See [Polaris qualification](../../deploy/polaris/qualification.json); FILE overrides are absent from the production renderer. |
| Actual API/backend combinations | Separate live tests combined the API and real PostgreSQL with Trino TLS/JWT and Polaris TLS/OAuth. They covered SQL exact results, notebook deletion/source retention, restrictive policy-version changes, native table schema discovery and grant revocation. This does not establish a combined production deployment or browser IdP flow. |
| Search | Actual PostgreSQL full-text/pgvector retrieval and a local TLS embedding gateway passed. Tests include filtering before ranking, metadata-only default transfer, model/dimension isolation, catalog grant/expiry checks, source changes during retrieval, publication after lease expiry, bounded source admission and no provider retry after unknown outcomes. No external provider was called; relevance/cost/load remain unqualified. |
| Workspace | Production build and 65 browser tests passed, including 14 Search tests, catalog and SQL coverage. A content-hashed source copy outside the iCloud-managed workspace avoided intermittent file hydration/HMR failures. Its Vite test configuration changed only the cache directory and filesystem allowlist for the temporary source/dependency symlink; the production container used the unchanged repository Vite configuration. A polling test was corrected to await fetched detail before advancing its fake clock. Desktop/mobile Search layouts were inspected. Browser backend fixtures remain labeled; they do not replace live engine tests. |
| SDK/CLI | Forty-eight tests and 12 real development API smoke groups passed at this checkpoint. They include SQL/catalog/search contracts, exact saved selections, uncertain-response recovery, safe namespace encoding and unknown-state handling. Subsequent Sylon transport tests are tracked separately until its backend integration is complete. |
| Deployment | Base 18, Spark 27, SQL 37, Catalog 36 and Search 40 unsafe-configuration cases passed with the full core Helm suite. Eight Trino profile/policy tests and five Polaris profile/render tests passed using locked upstream charts. EKS overlays rendered; no EKS cluster was deployed. |

The application base images are now locked by multiarchitecture manifest digest in [base-images.lock.json](../../deploy/base-images.lock.json), with CI rejecting unreviewed application Dockerfile bases. Runtime and upstream component locks remain separate. Rebuilding or rendering manifests is not runtime qualification of a later changed image.

After the final Sylon/container checkpoint, the task-owned local Kubernetes VM/data, PostgreSQL fixture, temporary credentials and task-installed extension files were removed. Unrelated profiles and installed developer tools were preserved. No AWS resources were created or changed. EKS/S3, production IdP, cloud lake interoperability, full installation/upgrade/restore, HA, workload scale and broader product capabilities remain separate gates.

## Local execution qualification — November 7–8, 2026

These checks cover the earlier local execution checkpoint following `57d1743`, before the ingestion/SQL/Sylon checkpoint above. They describe the source at evidence collection, not a released, supported production profile. The full roadmap remains open; current publication status is recorded separately.

| Check | Observed result and boundary |
|---|---|
| Durable API/controller | Full Go race suite: 69 top-level tests / 173 cases passed, including isolated PostgreSQL 17.11 integration; no test cases skipped. `go vet ./...` passed. The three packages with no tests are not skipped integration tests. Tests cover migrations, capacity, owner/workspace access, idempotency, fencing, revocation, deadlines, uncertain side effects, cancellation during a policy-file outage, bounded history, atomic notebook snapshots/concurrent edits and terminal source cleanup. |
| Workspace | TypeScript/Vite production build and 20 browser tests passed. Browser tests distinguish the real development API from controlled execution transport fixtures; fixtures do not certify Spark. Tests include saved-revision/dirty/conflict safeguards, persisted submission intent, malformed/wrong-owner receipts, recovery after edits/reload, cancellation, access revocation, workspace changes and continued cleanup observation after job completion. Desktop/mobile notebook submission and provenance layouts were inspected. The lazy editor bundle emits a nonblocking size warning (about 505 kB minified / 169 kB gzip); load-performance qualification remains outstanding. |
| SDK/CLI | Twenty-one tests and eight real API smoke groups passed, including notebook submit/source routes and exact-revision requests. Mutation requests do not retry automatically; watch timeout does not cancel work. The smoke launcher was run with an intentionally invalid inherited bindings path and correctly disabled it. |
| Helm and compute boundary | Default/managed renders, eighteen original and seventeen execution rejection cases passed. Ten compute-generator/verifier tests passed. Rendering is distinct from installing the complete chart. |
| Actual local Spark | Apache operator 1.0.0 / chart 1.8.0 and Spark 4.1.3 ran on isolated k3s `v1.35.0+k3s1` (ARM64). Real Python Spark example succeeded (`Pi is roughly 3.149440`); deliberate missing-file failure, UID-precondition cancellation, queued grant revocation, HTTP replay and owner-verified logs passed. The harness uses API handlers with explicit development authentication and memory storage. |
| Actual compute restrictions | Restricted PSA, native validating admission, quotas and network policies were enabled during the Spark run. All fifteen admission probes and six API/controller RBAC denials passed. An observed executor had UID/GID 185, non-root/RuntimeDefault settings, all capabilities dropped, token automount disabled and no projected token volume. Live probes resolved DNS, denied executor Kubernetes API/internet connections and denied changing the Spark role label. |
| Actual saved notebook | The [three-cell example](../../examples/python-batch-notebook.json) ran on the same real operator and produced exactly `{"rows":100,"sumSquares":328350}`, independently checked in Python. The harness accepted revision 1, edited revision 2, deleted the notebook and recovered the original request before reconciling; the executed source hash remained unchanged. Success, deliberate Python failure, explicit cancellation and a 60-second lifetime deadline all passed. Each source artifact reached persisted `deleted` state and its ConfigMap was absent. Owned source remained readable from the run snapshot in this memory harness; separate PostgreSQL tests cover durable persistence. A real failure traceback included the notebook/revision/cell filename. |
| Notebook source protection | Eighteen generic Pod/source-create admission probes, eight dedicated source mutation/deletion probes and eleven scoped authorization assertions passed. The actual driver mounted source read-only; its workload token could not delete protected source and the API token could not read ConfigMaps. Source inspection through DataSlate uses the owned run snapshot. The reusable verifier checks the exact source policy/binding and CREATE admission; a separate fixture verifier exercises UPDATE/DELETE and is part of the local harness. |
| OCI builds and startup | API, controller and workspace images built locally for Linux ARM64. API (UID 65532) and web (UID 101) started with read-only root filesystems; proxy health/readiness/status passed. Missing production configuration made API/controller exit unsuccessfully as expected. The startup check did not deploy production OIDC/PostgreSQL or qualify controller health with a production database. |
| Dependency audit | The patched workspace lockfile (Vite 8.3.6) reported zero known npm advisories at this snapshot. This is not a comprehensive security audit. |
| Combined source runner | `scripts/test-local.sh` passed from the actual repository after a clean `npm ci`, with the isolated PostgreSQL database enabled. This repeated the API, SDK, smoke, browser, compute and Helm checks together. |

The runtime images used were `apache/spark@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211` and `apache/spark-kubernetes-operator@sha256:349da72f95c2b7d1882729106f7b4a659803a8b6e8fd9be35ee3b8544ba7616b`. Generated driver/executor templates require numeric UID/GID 185: the initial live run exposed Kubernetes' inability to establish non-root status from this image's named `USER`; the corrected run passed. A different image identity needs separate runtime qualification.

The pinned Spark image reported Python 3.10.12 and OpenJDK Temurin 21.0.11+10-LTS. These observations describe the plain Spark batch image; they do not certify a Delta/Hadoop/S3 connector package combination. Independent review also identified an incomplete source-policy preflight and a known-expired operation being marked as an uncertain submission; both were corrected and covered by regression checks before the final suite.

The profile intentionally grants no PVC permissions and supports no PVC workloads. Spark emitted a PVC `deletecollection` cleanup denial while the application still completed successfully; that limitation remains visible rather than adding unused storage privileges.

Reproduce local operator/admission execution with `bash scripts/local-k8s-qualification.sh` after installing its stated prerequisites and downloading the pinned charts; select `QUALIFICATION_MODE=notebook` for the saved-notebook cases. The script refuses an existing profile of its fixed name, creates an isolated Colima VM, uses an explicit Kubernetes context and removes its VM/data/credential files on exit. Its individual provisioning steps and test harnesses were exercised, but the complete fresh-provision wrapper was not rerun end to end. Raw local evidence is retained under ignored `.qa/local-k8s/`; database and browser evidence are separate checks, not one integrated production-stack test.

The final Linux ARM64 image manifests built locally were:

| Image | Manifest digest |
|---|---|
| API | `sha256:480090152a893d04c2bde8ca5bec7cfb15e08af9ce00563d0f070e221f071926` |
| Controller | `sha256:1f13dd28ccaa3af72a3c78dd45bec33638996b71107e4c766dcfc588c2bb22c2` |
| Workspace | `sha256:8c63e465f313754f33b9ef9f1c2234261f9d965255c0743e3ae87e8fcc8f441b` |

These images were not published. The disposable Kubernetes VM/data, temporary Kubernetes credentials and isolated PostgreSQL test database were removed after qualification; installed developer tools remain available. A final targeted scan of 196 tracked/candidate text files found no private keys, AWS access-key identifiers, GitHub tokens or private host paths. That limited scan is not a comprehensive security audit. No AWS resources were created or changed.

**Outstanding qualification:** EKS/S3, IRSA, KMS, actual cloud file/table writes, production browser OIDC, complete Helm installation/upgrade/restore, governed SQL/RLS, interactive notebook sessions, ingestion, rich results, Sylon and the broader platform capabilities. Local k3s results do not establish EKS CNI/IAM behavior, a malicious-code sandbox, availability, capacity or an SLA.

## Publication validation — November 7, 2026

- [GitHub foundation CI](https://github.com/nimeshbuilds/dataslate/actions/runs/42430328346) passed all four jobs for implementation revision `6ea8cb2`: API tests/vet with isolated PostgreSQL and API image build; SDK tests and real API smoke; TypeScript/Vite build, browser tests and workspace image build; Helm/bootstrap validation. Image builds do not qualify deployed container behavior or a live Kubernetes installation.
- [GitHub Pages deployment](https://github.com/nimeshbuilds/dataslate/actions/runs/42430328285) passed. The public HTML, CSS, JavaScript, favicon and social image were fetched over HTTPS and matched repository bytes.
- The product site passed browser checks at 1440, 390 and 320 pixels: navigation, keyboard tabs, clipboard, internal anchors, image alternatives and overflow. Desktop/mobile visuals were inspected. Its product illustrations are labeled concepts with sample data.
- A clean temporary installation of the workspace dependencies passed its production build. Bundled font copyright/license files were verified unchanged in the build output.
- The initial committed history was checked for secret patterns, private absolute paths and generated dependencies before publication. No such publication blockers were found. This is a targeted review, not a comprehensive security audit.

## Original local snapshot — September 25, 2026

The following records the earlier local validation. Its pending remote-CI items were subsequently exercised as described above; live cloud and production qualification remain pending.

## Checks completed

| Check | Evidence and boundary |
|---|---|
| Go API | `go test -race ./...` and `go vet ./...` pass; handler authorization, cross-workspace access, concurrent/stale revisions and configuration boundaries are covered |
| JWT authentication | Signed test tokens through a JWKS test server cover signature, issuer, API audience, expiry, not-before, token-use and role behavior; this is not a live IdP integration |
| Development boundary | Non-loopback bind/Host/Origin and unsafe request media types are rejected by tests |
| Spark manifests | Apache and Kubeflow previews match their distinct API groups; validation against pinned upstream CRD schemas was performed. No job was submitted |
| API/SDK integration | `scripts/smoke.sh` starts a real disposable API and verifies save/reopen, conflict, scoped lookup, broader modules, dependency plan and both previews |
| Python SDK/CLI | Eight tests pass, including redirected credentials, denied requests, malformed/truncated HTTP, response bounds and preview output. Wheel build includes source and license; installed CLI runs |
| Workspace build | TypeScript and Vite production build pass |
| Browser workflows | Seven Playwright tests passed against the real API/Vite: persistence, conflict recovery, offline save, workspace scope, Spark previews, capability/planner/Sylon limits and mobile editor sizing |
| Visual review | Desktop and narrow workspace layouts, saved notebook/result tabs and AI-module views inspected; no page errors observed in review |
| Helm | External profile renders 12 resources; managed CNPG profile renders 13. Eighteen invalid configurations are rejected. Custom HTTPS/database-port policy rendering passes |
| Upstream bootstrap | Checksum-pinned Apache Spark and CNPG charts lint/render successfully offline after download |
| Dependency review | Frontend npm audit reported zero known vulnerabilities for the installed lockfile at this snapshot; this is not a comprehensive security assessment |
| CI configuration | Workflow added for Go/PostgreSQL, Python/API smoke, browser builds/tests, image builds and Helm/bootstrap checks. No remote CI run had occurred at this September snapshot |

Reproduce the source checks from the repository root:

```sh
make test-api
make test-sdk PYTHON=python3
PYTHON=python3 bash scripts/smoke.sh
cd web
npm ci
npm run build
npx playwright install chromium
npm test
```

Use Python 3.11+. Browser tests start their own API on 8080 and Vite on 5173 and require those ports to be free. For an existing local Chrome, set `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` instead of installing a browser. The [deployment guide](../../deploy/README.md) provides the isolated Helm setup/check commands.

After installing those prerequisites, `make test-local` combines the Go/race/vet, SDK, API smoke, workspace build/browser, compute-boundary and Helm checks. It requires `DATASLATE_TEST_DATABASE_URL` pointing at an isolated test database and fails rather than silently skipping the PostgreSQL integration. `PYTHON` selects an environment containing the deployment test requirements; `HELM_BIN` can override the checksum-verified local Helm binary. Live Kubernetes/OCI and EKS checks remain separate. The smoke/browser API launchers explicitly disable inherited execution bindings and use memory storage.

## Not executed at the original September snapshot

- Real PostgreSQL integration: the gated `DATASLATE_TEST_DATABASE_URL` test was skipped locally because no database was available. The workflow subsequently supplied an isolated test database in the November CI run above.
- Container builds/startup: no local Docker daemon is available. Dockerfiles and nginx/Helm contracts were reviewed; CI image-build jobs are configured.
- Live OIDC browser login, provider-specific claim mapping, session revocation and directory/group lifecycle.
- EKS installation, internal ALB, actual network-policy enforcement, S3 credentials, Spark execution and cloud table commits.
- CNPG failover, scheduled backup configuration, restore and upgrade/rollback drills.
- SQL/RLS/column masking, real result aggregation and exports, ingestion, Airflow, MLflow, Sylon/model calls, agents, memory, serving, features, apps and sharing. These are tracked implementation work, not silently simulated integrations.

The first live execution gate remains a supported EKS/S3 runtime plus governed UC/Delta/Spark SQL identity and policy enforcement. [Complete scope](coverage.md) retains every broader capability and its required acceptance evidence.

# Implementation validation

This records implementation evidence, not production certification. The local development application uses explicit loopback authentication and memory storage. The production code path requires OIDC and PostgreSQL.

## Local execution qualification — November 7–8, 2026

These checks cover the local execution checkpoint following `57d1743`. They do not describe the currently published GitHub Pages site or a released, supported production profile. The full roadmap remains open; this checkpoint has not been pushed.

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

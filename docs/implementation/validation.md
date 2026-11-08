# Foundation validation

This records implementation evidence, not production certification. The local development application uses explicit loopback authentication and memory storage. The production code path requires OIDC and PostgreSQL.

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

## Not executed locally

- Real PostgreSQL integration: the gated `DATASLATE_TEST_DATABASE_URL` test was skipped locally because no database was available. The workflow subsequently supplied an isolated test database in the November CI run above.
- Container builds/startup: no local Docker daemon is available. Dockerfiles and nginx/Helm contracts were reviewed; CI image-build jobs are configured.
- Live OIDC browser login, provider-specific claim mapping, session revocation and directory/group lifecycle.
- EKS installation, internal ALB, actual network-policy enforcement, S3 credentials, Spark execution and cloud table commits.
- CNPG failover, scheduled backup configuration, restore and upgrade/rollback drills.
- SQL/RLS/column masking, real result aggregation and exports, ingestion, Airflow, MLflow, Sylon/model calls, agents, memory, serving, features, apps and sharing. These are tracked implementation work, not silently simulated integrations.

The first live execution gate remains a supported EKS/S3 runtime plus governed UC/Delta/Spark SQL identity and policy enforcement. [Complete scope](coverage.md) retains every broader capability and its required acceptance evidence.

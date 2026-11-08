# DataSlate workspace

React/TypeScript application for the DataSlate foundation API. Sylon is the platform assistant's name. This application contains no simulated query results, assistant answers, successful connection probes or deployments.

## Run

Use Node.js 22 or newer. From `api/`, start the explicit loopback development service:

```sh
DATASLATE_AUTH_MODE=development go run ./cmd/dataslate-api
```

From `web/`:

```sh
npm ci
npm run dev
```

Open `http://127.0.0.1:5173`. Vite forwards `/api/` to `http://127.0.0.1:8080`. Development mode uses the backend's temporary administrator and in-memory storage; restarting the backend clears its documents. The UI labels this state. Do not expose this development server or API beyond loopback.

## Implemented behavior

- Workspace selection and administrator workspace creation through the API.
- Python/SQL document creation, CodeMirror editing, cell operations and real API save/reopen.
- Optimistic revision conflicts preserve unsaved edits and offer download or explicit reload.
- Connectivity errors preserve drafts in the current tab. A browser exit warning protects dirty documents.
- Saved SQL cell execution through an administrator-granted Trino binding. Exact notebook revision/cell selections, immutable SQL provenance/context/policy version, durable submission recovery, owner-scoped query history and observed cancellation use the query API.
- Table, Visualize, Profile and Execution tabs consume actual authorized SQL results. Tables preserve exact scalar strings and nulls with 50-row browser paging; complete results support bar/line charts and null/distinct profiles. Incomplete/truncated results disable charts and profiles. Large integers and decimals that would round are rejected for chart conversion, while raw table values remain intact.
- Polaris catalog browser with identity-scoped table-grant indexes, explicit discovery checks and actual Iceberg schema/snapshot metadata reads. Grant names are never presented as verified upstream existence; schema paging and exact snapshot strings preserve the returned metadata.
- Module explorer with separate implementation and installation status.
- Deployment plan generation, dependency inclusion, managed/external ownership and JSON export. The API performs validation; nothing is installed or probed.
- Apache/Kubeflow SparkApplication preview generation and export. Previews have no submission side effects.
- Approved application bindings and owner-scoped batch run history, observed timestamps, bounded events/logs and cancellation requests. Uncertain submissions retain their idempotency key for explicit recovery. Workspace/identity changes clear run data and stop old polling.
- Saved Python notebook batch submission through an explicit engineering binding, with a review of the saved revision/runtime identity, dirty/conflict safeguards, immutable source provenance and a link to the actual run. Later document edits do not replace a pending request's source selection.
- File ingestion pipeline create/list/read/rename/delete through the API, explicit JSON/Parquet schemas, immutable storage configuration, dedicated destination/checkpoint review and saved-revision batch launch. Ingestion snapshots and writer reservation cleanup appear in the shared run view.
- Private Sylon conversations with approved model/context profiles, exact saved-cell review, durable submission recovery, authorized citations and separate revision-checked code application.
- Responsive navigation, keyboard-addressable controls and dialogs with Escape/focus containment.

Interactive Python notebook sessions/cell results, Kafka/CDC, ingestion scheduling, BI, ML, agents and provider administration still require their backend integrations. Saved Python batches start a fresh process and expose actual lifecycle/logs; they do not fill the result tabs. The component explorer is a product-scope view, not an installed-services dashboard.

## Browser identity

`GET /api/v1/auth/config` determines development versus OIDC mode. Production uses `oidc-client-ts` Authorization Code + PKCE, callback `/auth/callback`, a public client ID and configured scopes. The API audience is sent as an authorization `audience` parameter; provider registration/scopes must actually mint a valid API access token. The backend independently verifies issuer, audience, signature, expiration and workspace claims. An ID token is never used as an API bearer token.

Unresolved batch submissions retain their binding ID and idempotency key in this tab’s sessionStorage, scoped by issuer, subject and workspace. Notebook requests additionally retain the selected notebook ID/revision in a notebook-specific recovery scope. Ingestion requests retain their selected pipeline ID/revision in a workspace-wide ingestion scope, so recovery remains reachable after a definition is deleted. SQL requests keep their binding ID, key and notebook/revision/cell selection in a separate workspace-scoped query recovery entry. They survive a reload and are recovered only after an explicit action; they contain no token, source, result or log. A validated matching receipt clears the entry. Closing the tab may discard it, so inspect existing runs before submitting a replacement after a browser crash.

Access/ID/refresh tokens remain in memory (`InMemoryWebStorage`); only temporary PKCE interaction state is in sessionStorage. No tokens are stored in localStorage, logged or added to URLs. Page reload requires another sign-in round trip; organization SSO may satisfy it. Automatic silent renewal and refresh-token persistence are deliberately disabled. A session-expiry banner preserves access to draft export before a new sign-in redirect. Unsaved document content itself is not persisted in browser storage.

The sign-out control clears DataSlate's local session; it does not sign the user out of all organizational applications. End-to-end production OIDC integration still requires a real identity-provider test.

## Container contract

Build with context `web/`. The unprivileged nginx image listens on port **8080**, serves the SPA and proxies API requests on the same origin. Set `DATASLATE_API_UPSTREAM` to the API service origin, e.g. `http://release-dataslate-api:8080`. This deployment value allows only an HTTP(S) host and optional port; credentials, paths and nginx syntax are rejected.

- Web liveness/readiness: `/web-healthz` (static nginx response).
- API health forwarding: `/healthz`, `/readyz`.
- SPA callback fallback: `/auth/callback`.
- Root filesystem may be read-only; mount a writable `emptyDir` at `/tmp`.
- Runtime UID/GID: `101:101`; no privilege escalation or capabilities required.
- Config, PID and nginx temp files live only beneath `/tmp/dataslate-nginx`.
- Bundled fonts and scripts require no runtime CDN.
- Access logs omit query strings so OAuth callback codes are not logged. Nginx's CSP permits HTTPS connections for configured OIDC endpoints and disallows framing/inline scripts. Scope `connect-src` more narrowly in a deployment-specific policy when the issuer origins are known.

## Verification

```sh
npm run build
npx playwright install chromium
npm test
```

The Playwright suite starts a real loopback Go API and Vite on ports 8080/5173, then shuts them down. Stop manually started servers on those ports before running it. It exercises save/reopen, real revision conflicts, offline recovery, workspace switching, both Spark operator previews, dependency planning, broader capability disclosure and mobile overflow. Notebook/source flows, ingestion definition CRUD/revision conflicts and disabled execution use the real API. Ingestion browser tests also verify immutable configuration, exact-revision receipts, recovery after definition deletion, rejected writer overlap and continued observation until writer cleanup is confirmed. SQL browser tests verify exact scalar preservation, complete-result chart/profile gates, paging, source selection, same-key recovery, cancellation, wrong-owner/mismatched-result rejection and access revocation. Catalog browser tests exercise the real unconfigured API, then explicit Polaris response fixtures for discovery/capability distinctions, exact snapshot IDs, policy mismatches, access loss, namespace races and workspace clearing. Execution UX recovery, capacity, scope and revocation cases use explicitly labeled transport fixtures; those tests do not prove live Spark execution. CI uses Playwright Chromium. To use an installed Chrome locally:

```sh
PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' npm test
```

The suite does not replace live EKS, object-storage, operator, container-runtime or OIDC qualification. No live cluster or provider is configured by these tests.

## Workspace search

Data → Search lists administrator-enabled search profiles. Lexical queries stay local; selecting hybrid explicitly discloses query transfer to the configured embedding gateway. Missing vector capability disables hybrid mode. Results show current authorized index entries, exact saved revisions, rank scores and catalog expiry; scores are not confidence probabilities and the index is not a completeness claim. Opening a match reloads the current notebook or table metadata through its access-controlled API.

Indexing reviews a saved notebook revision or an authorized catalog schema before submitting. The browser persists only an identity/workspace-scoped request intent and then the accepted job ID in session storage. Ambiguous receipts recover with the same key; storage failure prevents submission. Owner-only jobs are polled until observed completion, failure, unknown outcome or supersession. Revoked access clears displayed job data. No raw rows, private SQL outputs or notebook cell text are persisted in the browser recovery record. The API's enabled profiles may automatically index notebook saves; the UI does not issue implicit indexing requests.

Search tests exercise real workspace/document persistence and the real unconfigured search API, plus explicitly labeled search/index transport fixtures for embedding transfer disclosure, same-key recovery, wrong-owner rejection, stale revisions, source access loss, source navigation, UTF-8 limits and mobile layout. These tests do not call an external model or certify live pgvector/Polaris; backend integration tests cover the real database separately.

## Sylon private assistance

The Sylon drawer and notebook cell actions use administrator-approved assistant bindings. Starting a private conversation does not contact a model. Saved cell source requires an editor or administrator role; viewers retain generic and authorized schema questions. Each generation reviews its prompt, exact saved cell revision, up to four authorized catalog schemas and optional lexical/hybrid retrieval. Hybrid retrieval explicitly discloses its separate embedding transfer. Responses render as escaped text, with server-issued citation provenance, historical revision labels and token usage only when the provider reports it. No generated answer is treated as an executed action.

The browser stores an identity/workspace-scoped pending prompt/context intent only long enough to recover an uncertain receipt with the same key. It keeps the accepted conversation ID, not model output or cell source. Job observation stops at terminal/unknown states; access loss clears private history. Owner cancellation and deletion remain available where permitted after source revocation, with no claim that external processing or charges were reversed. The backend provides bounded conversation history and explicitly saved private memory; arbitrary tools, shared agent memory and model streaming remain separate work.

Applying code requires a separately reviewed exact-cell replacement. The UI re-reads the saved source, rejects a changed revision or language, blocks unresolved local edits and persists the exact apply intent before sending. Receipt recovery returns the original application without another write; the editor reload preserves any edits made during that read. A successful apply refreshes the selected cell revision for a follow-up question. SQL and Spark runs remain separate reviewed operations.

Sylon browser tests use real notebook/workspace persistence and the real unconfigured assistant API, plus explicit assistant transport fixtures. They exercise prompt/source review, owner and citation rejection, message/apply receipt recovery, a real saved notebook replacement, dirty/stale-cell guards, access revocation, cancellation/deletion, context allowlists, UTF-8 limits and narrow layouts. They do not bill a model or qualify EKS/provider deployment.

## Private Sylon memory

An assistant binding may explicitly enable private memory with default and maximum retention. Save a successful owned answer through **Save to private memory**, review its title/summary/expiry, and retain all original source permissions. There are no free-form unbound notes or automatic model extraction. The server owns the records; the browser stores only an unresolved mutation intent temporarily for exact same-key recovery, then removes it after a matching receipt.

The **Private memory** tab lists current authorized records and minimal owner-management metadata for unavailable records. Correction and expiry changes use expected revisions; deletion remains available where permitted after source revocation. Correcting, deleting or expiring memory closes conversation history derived from its earlier revision. Deleting the source conversation also deletes the memory. Applied notebook edits are not undone.

Recall is an explicit local lexical request scoped to the current assistant profile. Select up to four exact revisions; the message review re-reads their content, identity and expiry before sending. No automatic recall, provider call or attachment occurs. Memory provenance appears in the answer's server-issued citations. Open memory detail/correction, recalled-result and request-review views recheck access on visibility and approximately every ten seconds; content is cleared on an observed revision change, revocation or expiry. If that removes an unresolved correction request, only a bodyless request reference remains for inspection or explicit discard. Exact replay is no longer possible after this privacy check; a newer memory revision is not proof that the earlier correction succeeded. The server reauthorizes at every protected operation. Browser checks do not promise instantaneous removal of already downloaded information.

Memory browser tests cover reviewed save, correction conflict, exact create/correct/delete recovery, minimal revoked/expired responses, unsafe receipt rejection, explicit recall, source revocation, expiry during review, long retention timers, workspace clearing, UTF-8 bounds and mobile layout. Transport fixtures are labeled; they do not prove database durability or external model behavior.

The separate PostgreSQL/HTTPS browser qualification uses `npm run test:memory-live`. It requires an already running loopback harness plus `DATASLATE_MEMORY_STATE_FILE`, `DATASLATE_MEMORY_EVIDENCE_DIR` and `DATASLATE_MEMORY_PHASE=prepare|resume`; missing settings fail explicitly. The prepare phase saves and corrects a memory, then the harness owner restarts its process while retaining the database/profile. Resume verifies persistence, recalls the exact revision, generates through the HTTPS fixture and deletes memory with dependent-history denial. This is distinct from the ordinary mandatory browser suite and uses no route interception or external paid model.

## Licenses and attribution

Original DataSlate code is licensed under [Apache-2.0](../LICENSE). Bundled DM Sans and IBM Plex Mono fonts retain their SIL Open Font License 1.1 terms and copyright notices. The [font notice index](public/licenses/NOTICE.txt), [DM Sans license](public/licenses/dm-sans-OFL.txt) and [IBM Plex Mono license](public/licenses/ibm-plex-mono-OFL.txt) are copied into every production build under `/licenses/`.

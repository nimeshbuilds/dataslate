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
- Shared Table, Visualize, Profile and Execution tabs clearly explain the missing Result Service. They do not generate results or charts from fabricated data.
- Module explorer with separate implementation and installation status.
- Deployment plan generation, dependency inclusion, managed/external ownership and JSON export. The API performs validation; nothing is installed or probed.
- Apache/Kubeflow SparkApplication preview generation and export. The API never submits the manifest.
- Sylon component information and a disconnected-provider state, with no simulated chat.
- Responsive navigation, keyboard-addressable controls and dialogs with Escape/focus containment.

Charts, actual notebook/SQL execution, kernel sessions, live compute, ingestion, pipelines, BI, ML, agents and provider administration still require their backend integrations. The component explorer is a product-scope view, not an installed-services dashboard.

## Browser identity

`GET /api/v1/auth/config` determines development versus OIDC mode. Production uses `oidc-client-ts` Authorization Code + PKCE, callback `/auth/callback`, a public client ID and configured scopes. The API audience is sent as an authorization `audience` parameter; provider registration/scopes must actually mint a valid API access token. The backend independently verifies issuer, audience, signature, expiration and workspace claims. An ID token is never used as an API bearer token.

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

The Playwright suite starts a real loopback Go API and Vite on ports 8080/5173, then shuts them down. Stop manually started servers on those ports before running it. It exercises save/reopen, real revision conflicts, offline recovery, workspace switching, both Spark operator previews, dependency planning, broader capability disclosure and mobile overflow. It does not mock successful backend actions. CI uses Playwright Chromium. To use an installed Chrome locally:

```sh
PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' npm test
```

The suite does not replace live EKS, object-storage, operator, container-runtime or OIDC qualification. No live cluster or provider is configured by these tests.

## Licenses and attribution

Original DataSlate code is licensed under [Apache-2.0](../LICENSE). Bundled DM Sans and IBM Plex Mono fonts retain their SIL Open Font License 1.1 terms and copyright notices. The [font notice index](public/licenses/NOTICE.txt), [DM Sans license](public/licenses/dm-sans-OFL.txt) and [IBM Plex Mono license](public/licenses/ibm-plex-mono-OFL.txt) are copied into every production build under `/licenses/`.

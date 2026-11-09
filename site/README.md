# DataSlate product site

Repository-owned static site for <https://nimeshbuilds.github.io/dataslate/>. Plain HTML, CSS and JavaScript; no build or runtime dependencies, tracking, external fonts or third-party scripts.

The product illustration is an original HTML/CSS concept with sample data, explicitly labeled as a preview. The site distinguishes available foundation code from planned integration and production qualification. Keep those status statements aligned with `docs/implementation/coverage.md` when changing the page.

`quickstart.html` is the linked development and Kubernetes configuration guide.
Its deployment commands follow `deploy/quickstart.md` and the offline setup
generator in `deploy/setup/`. The guide includes source/image builds, OIDC
claims, separate migration/runtime identities, configuration generation,
read-only preflight, staged installation, first-workspace membership, optional
integration profiles and operational boundaries. The generator supports a
Secret-terminating ingress controller; the EKS ALB/ACM example is a separate
manually configured alternative. Do not imply that a generated bundle or
successful core installation qualifies the full platform for production. The current
local core installation evidence is in `deploy/core-installation-qualification.md`;
keep its exact runtime and test boundaries separate from EKS/S3 and optional engines.

## Local preview

From the repository root:

```sh
python3 -m http.server 4173 --directory site --bind 127.0.0.1
```

Open <http://127.0.0.1:4173/>. Relative asset paths also work under the GitHub Pages `/dataslate/` subpath. `index.html` contains canonical/social URLs, while `sitemap.xml` and `robots.txt` use the public URL.

Brand assets in `assets/` are copies of the repository's `brand/` sources. Update both when changing the identity. The Open Graph image is a 1280 × 640 PNG.

## Interaction and accessibility

- The mobile navigation exposes its expanded state, closes after navigation and supports Escape.
- Product, architecture and quickstart tabs support arrow keys, Home and End with roving focus.
- The command copy control reports success or selects text for manual copying if clipboard access fails.
- The full quickstart has deep links into either path, copyable command blocks,
  keyboard-focusable code/table scrolling, an expandable troubleshooting section
  and readable development/production instructions when JavaScript is disabled.
- A skip link, visible focus, landmarks, labeled sample content and reduced-motion behavior are included.
- All illustrations and charts here are sample concepts; this site does not call the DataSlate API or run queries.

The Pages workflow is maintained in `.github/workflows/` by the repository, rather than a site-specific hosting service.

## Browser verification

Use the workspace's pinned Playwright dependency after `npm ci` in `web/` and
installation of Chromium. From the repository root:

```sh
node site/check-browser.mjs
```

For an existing local Chrome, set `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` to its
executable. The check owns a loopback static server and browser and closes both.
It serves under `/dataslate/` to match GitHub Pages and verifies 1440, 768, 640, 390 and
320 px layouts, resources, anchors, repository documentation links, mobile navigation, path selection, keyboard
tabs, deep links, clipboard success/fallback and no-JavaScript reading. Local
screenshots are written only to ignored `.qa/site/`. It does not call a cluster,
IdP, registry, cloud API or model provider, and does not execute setup commands.

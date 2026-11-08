# DataSlate product site

Repository-owned static site for <https://nimeshbuilds.github.io/dataslate/>. Plain HTML, CSS and JavaScript; no build or runtime dependencies, tracking, external fonts or third-party scripts.

The product illustration is an original HTML/CSS concept with sample data, explicitly labeled as a preview. The site distinguishes available foundation code from planned integration and production qualification. Keep those status statements aligned with `docs/implementation/coverage.md` when changing the page.

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
- A skip link, visible focus, landmarks, labeled sample content and reduced-motion behavior are included.
- All illustrations and charts here are sample concepts; this site does not call the DataSlate API or run queries.

The Pages workflow is maintained in `.github/workflows/` by the repository, rather than a site-specific hosting service.

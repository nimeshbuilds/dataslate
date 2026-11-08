# DataSlate identity

DataSlate is an open data and AI workspace for Kubernetes. Its assistant is **Sylon**. The identity is original artwork for this repository, developed from the workspace's three rising slate layers. The stepped layers represent the journey from data to insight; they are not the closed diamond stack used by Databricks.

## Assets

| Asset | Use |
| --- | --- |
| `dataslate-wordmark.svg` | Full wordmark on white, paper or other light backgrounds |
| `dataslate-wordmark-dark.svg` | Full wordmark on pine or other dark backgrounds |
| `dataslate-wordmark-mono.svg` | Single-color print and constrained reproduction |
| `dataslate-mark.svg` / `dataslate-mark-dark.svg` | Compact product mark on light / dark backgrounds |
| `dataslate-mark-mono.svg` / `dataslate-mark-white.svg` | Single-color product mark |
| `sylon-mark.svg` / `sylon-mark-dark.svg` | Assistant mark; pair with the Sylon name until context makes its identity clear |
| `favicon.svg`, `favicon-32.png`, `apple-touch-icon.png` | Browser and touch icons |
| `github-banner.svg` / `github-banner.png` | 1280 × 400 repository introduction |
| `social-card.svg` / `social-card.png` | 1280 × 640 repository social preview and website Open Graph image |

The SVGs are the editable sources. All source assets are self-contained and make no network requests. Wordmarks use live text with an Inter/Arial/sans-serif fallback; the checked-in PNGs provide predictable typography for link previews. PNG artwork is an export of the SVG, not a separate identity.

## Color

| Token | Hex | Role |
| --- | --- | --- |
| Pine | `#123D32` | Primary mark, headings and strong controls |
| Deep ink | `#102B25` | Dark sections and repository banner |
| Slate green | `#56836D` | Secondary layer and restrained accents |
| Soft sage | `#91BBA0` | Third layer and decorative detail |
| Sage surface | `#DCE9DD` | Supporting panels |
| Fern | `#B9E29E` | Accent on dark surfaces |
| Warm paper | `#F7F8F2` | Primary light surface |

Use pine or deep ink for body text on light surfaces. Fern and soft sage are decorative colors, not small text on white. Always test the actual foreground/background pair for accessibility. Color must not be the only signal for status.

## Type and spacing

Use a modern system sans-serif stack, optionally Inter when it is already bundled. No font download is needed for the identity. Use a monospace font only for code, identifiers and compact technical metadata. Keep a clear hierarchy and generous whitespace; avoid dashboard clutter on marketing pages.

Keep at least one stripe's height of clear space around the mark. Use the compact mark at 24 CSS pixels wide or larger; the favicon has its own padded container for small sizes. Do not stretch, rotate, outline or add gradients to the mark. Use a supplied dark or monochrome version when a background changes. Do not combine the symbol with a different product name or imply third-party endorsement.

Write **DataSlate** and **Sylon**, preserving capitalization. The wordmark's trailing dot is a visual element; the product name in prose is DataSlate.

## Voice and product status

Lead with the work people can do: connect their lake, author notebooks and SQL, understand runs, and operate their infrastructure. Describe planned capabilities as planned. The initial public repository is a foundation build; its banner intentionally states that stage. A deployment manifest, integration definition or preview is not evidence of a live backend integration.

Preferred introduction: **“An open data + AI workspace for Kubernetes.”**

Core line: **“Your lake. Your workspace.”**

Sylon is a companion for governed data and code work. Its guiding-light symbol should feel helpful and precise, without promising infallible answers or granting authority to act without product policy.

## Accessibility and implementation

Standalone SVGs have a title and description. When placed next to a visible name, use empty image alt text (or `aria-hidden="true"` for inline SVG) to avoid duplicate announcements. When a logo is the only content of a link, name the destination, for example `aria-label="DataSlate home"`. The application uses the same slate paths and Sylon symbol as these source files.

The application browser icons are copied to `web/public/brand/`; the GitHub Pages site copies public-facing assets into its own asset directory. Update those copies when changing the source identity.

Artwork is distributed with this repository under its Apache-2.0 license; see the root `LICENSE`. This documentation is not a claim of trademark registration, exclusivity or legal clearance.

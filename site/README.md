# firn website

Zero-build static site for [firn](https://github.com/rahulkj/firn), deployed to GitHub Pages at `https://rahulkj.github.io/firn/` by `.github/workflows/site.yml`.

## Layout

```
site/
  index.html            landing
  404.html
  favicon.svg
  assets/style.css      design system (dark default, light via [data-theme="light"])
  assets/site.js        theme toggle, mobile nav, copy buttons
  docs/                 hand-written pages mirroring docs/ in the repo
```

No build step, no npm, no dependencies — open `index.html` or serve statically:

```sh
python3 -m http.server 8080 -d site   # then http://localhost:8080
```

## Keeping pages honest

Pages are hand-written and mirror the repository's canonical docs. When you change behavior, update **both** in the same commit:

| Page | Source of truth |
|---|---|
| docs/api.html | docs/api.md, internal/api/firn.proto |
| docs/comparison.html | docs/comparison.md |
| docs/architecture.html | docs/adr/012-architecture.md |
| docs/configuration.html | docs/configuration.md, `firnd -h` |
| docs/sre.html | docs/sre.md, internal/metrics/metrics.go |
| docs/releases.html | CHANGELOG.md |
| docs/security.html | docs/adr/011-secure-mode.md, docs/distribution.md |

Diagram pages (architecture, security) render Mermaid from CDN with the same
fences used in the ADRs — if JS is disabled, the diagram source still reads as text.

When you ship a new release, bump the `<span class="ver">` chips and any
version-stamped examples (`firn-vX.Y.Z` filenames) in the same commit.

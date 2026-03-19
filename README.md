# xBound Demo

This repo contains the Electron demo and a static web build for GitHub Pages.

## Local Run (Electron)

```bash
npm install
npm start
```

## Build Static Site (for GitHub Pages)

```bash
npm run build:web
```

This generates `dist/` with:
- `index.html`
- `renderer.js`
- `styles.css`
- `icons/`
- `data/`
- `.nojekyll`

## Deploy Model Used Here

GitHub Pages is configured as:
- **Source**: `Deploy from a branch`
- **Branch**: `gh-pages`
- **Folder**: `/(root)`

The workflow file [`.github/workflows/deploy-gh-pages.yml`](.github/workflows/deploy-gh-pages.yml) does:
1. Build with `npm run build:web`
2. Publish `dist/` to branch `gh-pages`

## Commands Used to Deploy

### Automatic deploy (recommended)

Push to `main` and let Actions update `gh-pages`:

```bash
git add .
git commit -m "Your message"
git push upstream main
```

### Manual deploy (if needed)

```bash
npm run build:web
git checkout gh-pages
git rm -rf .
cp -R dist/. .
git add .
git commit -m "Manual Pages update"
git push upstream gh-pages
git checkout main
```

## Notes

- Electron and web use the same precomputed `.jsonl` files under `data/benchmarks/...`.
- Custom query estimation is Electron-only (web does not run DB backends).

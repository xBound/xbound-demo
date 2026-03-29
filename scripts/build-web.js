const fs = require('fs/promises');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const DIST = path.join(ROOT, 'dist');

async function cleanDist() {
  await fs.rm(DIST, { recursive: true, force: true });
  await fs.mkdir(DIST, { recursive: true });
}

async function copyFile(src, dest) {
  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.copyFile(src, dest);
}

async function copyDir(src, dest) {
  await fs.mkdir(dest, { recursive: true });
  const entries = await fs.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    const from = path.join(src, entry.name);
    const to = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      await copyDir(from, to);
    } else if (entry.isFile()) {
      await copyFile(from, to);
    }
  }
}

async function build() {
  await cleanDist();
  // Cache-bust static assets: Chrome on GitHub Pages may keep stale JS/CSS across deploys.
  const buildVersion = (process.env.GITHUB_SHA || `${Date.now()}`).slice(0, 12);

  const srcIndexPath = path.join(ROOT, 'src', 'index.html');
  const srcIndex = await fs.readFile(srcIndexPath, 'utf8');

  const webIndex = srcIndex
    .replaceAll('../icons/', './icons/')
    .replaceAll('../images/', './images/')
    .replace('href="./styles.css"', `href="./styles.css?v=${buildVersion}"`)
    .replace('src="./renderer.js"', `src="./renderer.js?v=${buildVersion}"`)
    .replace(
      '<script src="./renderer.js',
      `<script>window.__XBOUND_BUILD_VERSION__ = "${buildVersion}";</script>\n    <script src="./renderer.js`
    );

  await fs.writeFile(path.join(DIST, 'index.html'), webIndex, 'utf8');
  await copyFile(path.join(ROOT, 'src', 'styles.css'), path.join(DIST, 'styles.css'));
  await copyFile(path.join(ROOT, 'src', 'renderer.js'), path.join(DIST, 'renderer.js'));
  await copyDir(path.join(ROOT, 'icons'), path.join(DIST, 'icons'));
  await copyDir(path.join(ROOT, 'images'), path.join(DIST, 'images'));
  await copyDir(path.join(ROOT, 'data'), path.join(DIST, 'data'));
  await fs.writeFile(path.join(DIST, '.nojekyll'), '', 'utf8');

  console.log('Built web assets to dist/');
}

build().catch((err) => {
  console.error(err);
  process.exit(1);
});

import { cp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DIST = path.join(__dirname, 'dist');
const SITE = path.join(__dirname, 'site');
const ASSETS = path.join(__dirname, 'assets');
const PARTIALS = path.join(__dirname, 'partials');
const STYLES = path.join(__dirname, 'styles');

async function build() {
  await rm(DIST, { force: true, recursive: true });
  await mkdir(DIST, { recursive: true });

  await cp(path.join(SITE, 'index.html'), path.join(DIST, 'index.html'));
  await cp(ASSETS, path.join(DIST, 'assets'), { recursive: true });
  await cp(PARTIALS, path.join(DIST, 'partials'), { recursive: true });
  await cp(STYLES, path.join(DIST, 'styles'), { recursive: true });

  console.log('Build complete: dist/ is ready.');
}

build().catch((error) => {
  console.error('Build failed:', error);
  process.exit(1);
});

import { cp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DIST = path.resolve(__dirname, 'dist');
const SITE = path.resolve(__dirname, 'site');
const ASSETS = path.resolve(__dirname, 'assets');
const PARTIALS = path.resolve(__dirname, 'partials');
const STYLES = path.resolve(__dirname, 'styles');

async function build() {
  console.log('BUILD __dirname =', __dirname);
  console.log('BUILD DIST =', DIST);

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

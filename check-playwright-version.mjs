import fs from 'node:fs/promises';

const expected = '1.56.1';

const raw = await fs.readFile(new URL('./package.json', import.meta.url), 'utf8');
const pkg = JSON.parse(raw);
const actual = pkg?.dependencies?.playwright;

if (actual !== expected) {
    // Fail fast so we don't ship an image with a mismatched Playwright version.
    // The base image tag is pinned to Playwright 1.56.1.
    throw new Error(`Playwright version mismatch: package.json has playwright=${actual}, expected ${expected}`);
}

console.log(`Playwright version OK: ${actual}`);

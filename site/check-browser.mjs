// Repository-only QA: uses the workspace's pinned Playwright dependency.
// No browser or runtime dependency is shipped to the static product site.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { access, readFile, mkdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const siteRoot = dirname(fileURLToPath(import.meta.url));
const require = createRequire(new URL('../web/package.json', import.meta.url));
const { chromium, expect } = require('@playwright/test');
const mime = { '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.png': 'image/png' };
const server = createServer(async (request, response) => {
  try {
    const pathname = decodeURIComponent(new URL(request.url, 'http://localhost').pathname);
    if (!pathname.startsWith('/dataslate/')) throw new Error('Outside site');
    const relative = pathname.slice('/dataslate/'.length) || 'index.html';
    const target = resolve(siteRoot, relative);
    if (!target.startsWith(`${siteRoot}${sep}`)) throw new Error('Outside site');
    response.setHeader('Content-Type', mime[extname(target)] ?? 'application/octet-stream');
    response.end(await readFile(target));
  } catch {
    response.writeHead(404).end('Not found');
  }
});
await new Promise((resolveReady) => server.listen(0, '127.0.0.1', resolveReady));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch({ executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined });
const failures = [];
let checks = 0;
try {
  const context = await browser.newContext({ permissions: ['clipboard-read', 'clipboard-write'], reducedMotion: 'reduce' });
  const page = await context.newPage();
  const repositoryLinks = new Set();
  page.on('pageerror', (error) => failures.push(error.message));
  page.on('response', (response) => { if (response.url().startsWith(origin) && response.status() >= 400) failures.push(`${response.status()} ${response.url()}`); });

  for (const width of [1440, 768, 640, 390, 320]) {
    await page.setViewportSize({ width, height: 1000 });
    for (const path of ['', 'quickstart.html', 'quickstart.html#production']) {
      await page.goto(`${origin}/dataslate/${path}`);
      for (const link of await page.locator('a[href]').evaluateAll((links) => links.map((link) => link.href))) {
        const source = new URL(link);
        const match = source.pathname.match(/^\/NimeshBuilds\/dataslate\/(?:blob|tree)\/main\/(.+)$/i);
        if (source.hostname === 'github.com' && match) repositoryLinks.add(decodeURIComponent(match[1]));
      }
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, `Page overflow: ${width}px ${path}`);
      assert.equal(await page.locator('img').evaluateAll((images) => images.every((image) => image.complete && image.naturalWidth > 0)), true);
      const anchorErrors = await page.locator('a[href^="#"]').evaluateAll((links) => links.flatMap((link) => {
        const href = link.getAttribute('href');
        return href.length > 1 && !document.getElementById(href.slice(1)) ? [href] : [];
      }));
      assert.deepEqual(anchorErrors, [], `Broken anchors: ${path}`);
      const identifiers = await page.locator('[id]').evaluateAll((elements) => elements.map((element) => element.id));
      assert.equal(new Set(identifiers).size, identifiers.length, `Duplicate element IDs: ${path}`);
      assert.deepEqual(await page.locator('[data-copy]').evaluateAll((buttons) => buttons.flatMap((button) => {
        const code = document.getElementById(button.dataset.copy);
        return code?.textContent.trim() ? [] : [button.dataset.copy];
      })), [], `Missing copy target: ${path}`);
      if (path.includes('quickstart')) {
        const selected = path.includes('#production') ? 'production' : 'development';
        assert.equal(await page.locator(`#${selected}`).isVisible(), true);
        assert.equal(await page.locator(`#${selected === 'production' ? 'development' : 'production'}`).isVisible(), false);
      }
      if (width <= 390) {
        const menu = page.getByRole('button', { name: 'Menu', exact: true });
        await menu.click();
        assert.equal(await menu.getAttribute('aria-expanded'), 'true');
        await page.keyboard.press('Escape');
        assert.equal(await menu.getAttribute('aria-expanded'), 'false');
      }
      checks++;
    }
  }
  for (const link of repositoryLinks) {
    const target = resolve(siteRoot, '..', link);
    assert.ok(target.startsWith(`${resolve(siteRoot, '..')}${sep}`));
    await access(target).catch(() => assert.fail(`Missing repository documentation link: ${link}`));
  }
  checks++;

  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`${origin}/dataslate/quickstart.html`);
  const devTab = page.getByRole('tab', { name: 'Development', exact: false });
  const prodTab = page.getByRole('tab', { name: 'Kubernetes / production config', exact: false });
  await devTab.focus();
  await page.keyboard.press('ArrowRight');
  assert.equal(await prodTab.getAttribute('aria-selected'), 'true');
  assert.equal(new URL(page.url()).hash, '#production');
  assert.equal(await prodTab.evaluate((element) => document.activeElement === element), true);
  await page.keyboard.press('Home');
  assert.equal(await devTab.getAttribute('aria-selected'), 'true');
  await page.keyboard.press('End');
  assert.equal(await prodTab.getAttribute('aria-selected'), 'true');
  await page.keyboard.press('ArrowRight');
  assert.equal(await devTab.getAttribute('aria-selected'), 'true');
  checks++;

  await page.getByRole('button', { name: 'Copy API startup commands', exact: true }).click();
  assert.equal(await page.evaluate(() => navigator.clipboard.readText()), await page.locator('#dev-api-code').textContent());
  assert.match(await page.locator('#guide-copy-status').textContent(), /copied to clipboard/);
  await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined }));
  await page.getByRole('button', { name: 'Copy workspace startup commands', exact: true }).click();
  assert.equal(await page.evaluate(() => window.getSelection().toString()), await page.locator('#dev-web-code').textContent());
  assert.match(await page.locator('#guide-copy-status').textContent(), /Commands are selected/);
  checks++;

  await page.goto(`${origin}/dataslate/quickstart.html#prod-integrations`);
  assert.equal(await page.locator('#production').isVisible(), true);
  assert.equal(await page.locator('#development').isVisible(), false);
  assert.equal(await page.locator('#prod-integrations').evaluate((element) => element.getBoundingClientRect().top >= 80 && element.getBoundingClientRect().top < 180), true);
  await page.goto(`${origin}/dataslate/quickstart.html#%broken`);
  assert.equal(await page.locator('.guide-panel:not([hidden])').count(), 1);
  await devTab.click();
  await page.getByRole('link', { name: 'Continue to Kubernetes' }).click();
  await expect(page.locator('#production')).toBeVisible();
  await page.getByRole('link', { name: 'Troubleshooting', exact: true }).click();
  await page.getByText('Preflight returns 3.', { exact: true }).click();
  assert.equal(await page.locator('details[open]').count(), 1);
  checks++;

  const noScript = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 390, height: 900 } });
  const plainPage = await noScript.newPage();
  await plainPage.goto(`${origin}/dataslate/quickstart.html`);
  assert.equal(await plainPage.locator('#development').isVisible(), true);
  assert.equal(await plainPage.locator('#production').isVisible(), true);
  await noScript.close();
  checks++;

  const evidence = resolve(siteRoot, '../.qa/site');
  await mkdir(evidence, { recursive: true });
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: width === 1440 ? 1100 : 1000 });
    await page.goto(`${origin}/dataslate/quickstart.html`);
    await page.screenshot({ path: `${evidence}/quickstart-${width}.png`, fullPage: false });
    await page.goto(`${origin}/dataslate/quickstart.html#production`);
    await page.screenshot({ path: `${evidence}/production-${width}.png`, fullPage: false });
  }
  assert.deepEqual(failures, [], 'Browser/resource errors');
  console.log(`PASS ${checks} site browser scenarios: responsive pages, assets, anchors, ${repositoryLinks.size} repository links, tab/deep-link navigation, clipboard/fallback, mobile menu and no-JavaScript reading.`);
  await context.close();
} finally {
  await browser.close();
  await new Promise((resolveClosed) => server.close(resolveClosed));
}

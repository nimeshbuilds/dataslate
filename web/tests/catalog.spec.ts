import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { CatalogBinding, CatalogTable, TableIdentifier } from '../src/catalog-api';

// Real workspace/auth/document API; explicit Polaris transport fixtures below.
// These tests validate browser behavior and do not qualify a live Polaris cluster.
const binding: CatalogBinding = { id: 'lake-catalog', workspaceId: 'default', name: 'Company lake', kind: 'polaris', catalog: 'lakehouse', policyVersion: 'v1', discoveryVerified: false };
const table: CatalogTable = { bindingId: binding.id, workspaceId: 'default', policyVersion: 'v1', identifier: { namespace: ['analytics'], name: 'orders' }, formatVersion: 2, uuid: 'cc0af00c-c2a6-4048-b671-d59cd41371ef', lastUpdatedMs: 1794052800000, currentSnapshotId: '9223372036854775806', currentSchema: { id: 2, fields: [{ id: 1, name: 'order_id', required: true, type: 'long' }, { id: 2, name: 'amount', required: false, type: 'decimal(38,9)' }, { id: 3, name: 'tags', required: false, type: { type: 'list', 'element-id': 4, element: 'string', 'element-required': false } }] } };
async function fixture(page: Page, transform?: (value: CatalogTable) => CatalogTable) {
  const requests: TableIdentifier[] = [];
  await page.route('**/api/v1/workspaces/default/catalog-bindings', (route) => route.fulfill({ json: { items: [binding] } }));
  await page.route('**/api/v1/workspaces/default/catalog-bindings/lake-catalog/namespaces', (route) => route.fulfill({ json: { source: 'authorized-grants', items: [{ namespace: ['analytics'] }, { namespace: ['engineering', 'daily'] }] } }));
  await page.route('**/api/v1/workspaces/default/catalog-bindings/lake-catalog/tables?**', (route) => { const namespace = JSON.parse(new URL(route.request().url()).searchParams.get('namespace')!); return route.fulfill({ json: { source: 'authorized-grants', items: [{ namespace, name: namespace[0] === 'analytics' ? 'orders' : 'events' }] } }); });
  await page.route('**/api/v1/workspaces/default/catalog-bindings/lake-catalog/table', (route) => { const input = route.request().postDataJSON() as TableIdentifier; requests.push(input); const value = { ...table, identifier: input }; return route.fulfill({ json: transform?.(value) ?? value }); });
  await page.route('**/api/v1/workspaces/default/catalog-bindings/lake-catalog/verify', (route) => route.fulfill({ json: { bindingId: binding.id, workspaceId: 'default', verified: true, checkedAt: '2026-11-07T12:00:00Z', capabilities: ['GET /v1/{prefix}/namespaces/{namespace}/tables/{table}'] } }));
  return requests;
}
async function open(page: Page) { await page.goto('/'); await page.getByRole('button', { name: 'Data', exact: true }).click(); await expect(page.getByRole('heading', { name: 'Your lake, connected.' })).toBeVisible(); }

test('catalog unconfigured real API exposes no invented connection or metadata', async ({ page, request }) => {
  const response = await request.get('/api/v1/workspaces/default/catalog-bindings'); expect(response.ok()).toBeTruthy(); expect(await response.json()).toEqual({ items: [] }); await open(page); await expect(page.getByRole('heading', { name: 'No catalog access configured' })).toBeVisible(); await expect(page.getByRole('region', { name: 'Table metadata' })).toHaveCount(0);
});

test('catalog separates authorized grant index from actual metadata and preserves snapshot precision', async ({ page }) => {
  const requests = await fixture(page); await open(page); await expect(page.getByText('Discovery not confirmed', { exact: true })).toBeVisible(); await expect(page.getByText('Names do not confirm that a table exists upstream', { exact: false })).toBeVisible(); expect(requests).toEqual([]); await expect(page.getByRole('heading', { name: 'Inspect an authorized table' })).toBeVisible();
  await page.getByRole('button', { name: 'orders', exact: true }).click(); const metadata = page.getByRole('region', { name: 'Table metadata', exact: true }); await expect(metadata.getByText('9223372036854775806', { exact: true })).toBeVisible(); await expect(metadata.getByRole('rowheader', { name: 'order_id' })).toBeVisible(); await expect(metadata.getByText('decimal(38,9)', { exact: true })).toBeVisible(); await expect(metadata.getByText('"element-id":4', { exact: false })).toBeVisible(); expect(requests).toEqual([{ namespace: ['analytics'], name: 'orders' }]); await page.screenshot({ path: '/tmp/dataslate-catalog-desktop.png', fullPage: true });
});

test('catalog discovery confirmation does not claim table existence or metadata capability', async ({ page }) => {
  const requests = await fixture(page); await page.route('**/catalog-bindings/lake-catalog/verify', (route) => route.fulfill({ json: { bindingId: binding.id, workspaceId: 'default', verified: true, checkedAt: '2026-11-07T12:00:00Z', capabilities: [] } })); await page.route('**/catalog-bindings/lake-catalog/table', (route) => route.fulfill({ status: 501, json: { error: { code: 'catalog_capability_unavailable', message: 'Polaris did not advertise table metadata reads.' } } })); await open(page); await page.getByRole('button', { name: 'Check catalog', exact: true }).click(); await expect(page.getByText('Discovery checked', { exact: true })).toBeVisible(); await expect(page.getByText('No table-read capability was advertised.', { exact: false })).toBeVisible(); expect(requests).toEqual([]); await page.getByRole('button', { name: 'orders', exact: true }).click(); await expect(page.getByRole('alert')).toContainText('did not advertise table metadata reads'); await expect(page.getByText(table.uuid, { exact: true })).toHaveCount(0);
});

test('catalog 403 refresh clears metadata already shown', async ({ page }) => {
  await fixture(page); await open(page); await page.getByRole('button', { name: 'orders', exact: true }).click(); await expect(page.getByText(table.uuid, { exact: true })).toBeVisible(); await page.route('**/catalog-bindings/lake-catalog/table', (route) => route.fulfill({ status: 403, json: { error: { code: 'catalog_access_denied', message: 'Catalog access was revoked.' } } })); await page.getByRole('button', { name: 'Refresh table metadata', exact: true }).click(); await expect(page.getByRole('alert')).toContainText('Catalog access was revoked'); await expect(page.getByText(table.uuid, { exact: true })).toHaveCount(0); await expect(page.getByRole('rowheader', { name: 'order_id' })).toHaveCount(0);
});

test('catalog rejects metadata belonging to another policy or table', async ({ page }) => {
  await fixture(page, (value) => ({ ...value, policyVersion: 'previous-policy', currentSchema: { id: 0, fields: [{ id: 1, name: 'private_field', type: 'string', required: false }] } })); await open(page); await page.getByRole('button', { name: 'orders', exact: true }).click(); await expect(page.getByRole('alert')).toContainText('different table or policy'); await expect(page.getByText('private_field', { exact: true })).toHaveCount(0); await expect(page.getByText(table.uuid, { exact: true })).toHaveCount(0);
});

test('catalog namespace switches discard late metadata from the previous selection', async ({ page }) => {
  await fixture(page); let release: () => void = () => {}; const held = new Promise<void>((resolve) => { release = resolve; }); let started = false;
  await page.route('**/catalog-bindings/lake-catalog/table', async (route) => { const input = route.request().postDataJSON(); if (input.name === 'orders') { started = true; await held; } await route.fulfill({ json: { ...table, identifier: input } }).catch(() => {}); }); await open(page); await page.getByRole('button', { name: 'orders', exact: true }).click(); await expect.poll(() => started).toBeTruthy(); await page.getByRole('combobox', { name: 'Catalog namespace' }).selectOption(JSON.stringify(['engineering', 'daily'])); await page.getByRole('button', { name: 'events', exact: true }).click(); release(); const metadata = page.getByRole('region', { name: 'Table metadata' }); await expect(metadata.getByRole('heading', { name: 'events', exact: true })).toBeVisible(); await expect(metadata.getByRole('heading', { name: 'orders', exact: true })).toHaveCount(0);
});

test('catalog workspace changes clear metadata and reload only current workspace bindings', async ({ page, request }) => {
  const slug = `catalog-${Date.now()}`; const response = await request.post('/api/v1/workspaces', { data: { name: slug, slug } }); expect(response.ok()).toBeTruthy(); const workspace = await response.json(); await fixture(page); await open(page); await page.getByRole('button', { name: 'orders', exact: true }).click(); await expect(page.getByText(table.uuid, { exact: true })).toBeVisible(); await page.getByRole('combobox', { name: 'Workspace', exact: true }).selectOption(workspace.id); await expect(page.getByRole('heading', { name: 'No catalog access configured' })).toBeVisible(); await expect(page.getByText(table.uuid, { exact: true })).toHaveCount(0); await expect(page.getByRole('button', { name: 'Company lake', exact: false })).toHaveCount(0);
});

test('catalog field rendering is bounded and usable at narrow viewport widths', async ({ page }) => {
  await fixture(page, (value) => ({ ...value, currentSchema: { id: 3, fields: Array.from({ length: 101 }, (_, index) => ({ id: index + 1, name: `field_${index + 1}`, type: 'string', required: false })) } })); await page.setViewportSize({ width: 390, height: 844 }); await open(page); await page.getByRole('button', { name: 'orders', exact: true }).click(); const metadata = page.getByRole('region', { name: 'Table metadata' }); await expect(metadata.locator('tbody tr')).toHaveCount(50); await expect(metadata.getByRole('rowheader', { name: 'field_1', exact: true })).toBeVisible(); await metadata.getByRole('button', { name: 'Next schema fields' }).click(); await expect(metadata.getByRole('rowheader', { name: 'field_51', exact: true })).toBeVisible(); await expect(metadata.getByRole('rowheader', { name: 'field_1', exact: true })).toHaveCount(0); const widths = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth })); expect(widths.scroll).toBeLessThanOrEqual(widths.client); await page.screenshot({ path: '/tmp/dataslate-catalog-narrow.png', fullPage: true });
});

test('catalog grant index cannot masquerade as upstream discovery', async ({ page }) => {
  await fixture(page); await page.route('**/catalog-bindings/lake-catalog/namespaces', (route) => route.fulfill({ json: { source: 'upstream', items: [{ namespace: ['private'] }] } })); await open(page); await expect(page.getByRole('alert')).toContainText('invalid catalog grant index'); await expect(page.getByRole('combobox', { name: 'Catalog namespace' })).toHaveCount(0);
});

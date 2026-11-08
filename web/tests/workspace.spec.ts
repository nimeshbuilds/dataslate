import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

const unique = () => `test-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
async function ready(page: Page) {
  await page.goto('/');
  await expect(page.getByRole('button', { name: 'Create notebook', exact: true })).toBeVisible();
}
async function saveNew(page: Page, name: string, source = 'value = 42\nprint(value)') {
  await page.getByRole('button', { name: 'Create notebook', exact: true }).click();
  await page.getByRole('textbox', { name: 'Document name' }).fill(name);
  await page.locator('.cm-content').first().fill(source);
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByText('Saved · revision 1')).toBeVisible();
}

test('save and reopen a notebook against the real API; show honest shared result views', async ({ page, request }) => {
  const name = unique();
  await ready(page); await saveNew(page, name);
  await page.getByRole('tab', { name: 'Visualize', exact: true }).click();
  await expect(page.getByText('From results to a clearer picture')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Run all', exact: true })).toBeDisabled();
  await page.reload();
  await page.getByRole('button', { name: `${name} Notebook · revision 1` }).click();
  await expect(page.locator('.cm-content').first()).toContainText('value = 42');
  const response = await request.get('/api/v1/workspaces/default/notebooks');
  expect(response.ok()).toBeTruthy();
  const saved = (await response.json()).items.find((item: { name: string }) => item.name === name);
  expect(saved.revision).toBe(1);
  expect(saved.cells[0].source).toContain('print(value)');
  expect(saved).not.toHaveProperty('outputs');
});

test('stale revisions preserve local edits and require explicit conflict resolution', async ({ page, request }) => {
  const name = unique(); await ready(page); await saveNew(page, name);
  const list = await (await request.get('/api/v1/workspaces/default/notebooks')).json();
  const saved = list.items.find((item: { name: string }) => item.name === name);
  const remoteCells = [{ ...saved.cells[0], source: 'remote_change = True' }];
  const remote = await request.put(`/api/v1/workspaces/default/notebooks/${saved.id}`, { data: { name, kind: saved.kind, cells: remoteCells, revision: saved.revision } });
  expect(remote.ok()).toBeTruthy();
  await page.locator('.cm-content').first().fill('my_unsaved_change = True');
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByText('A newer revision exists.', { exact: false })).toBeVisible();
  await expect(page.locator('.cm-content').first()).toContainText('my_unsaved_change');
  await expect(page.getByRole('button', { name: 'Download my edits' })).toBeVisible();
  const after = await (await request.get(`/api/v1/workspaces/default/notebooks/${saved.id}`)).json();
  expect(after.cells[0].source).toBe('remote_change = True');
  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Reload saved revision' }).click();
  await expect(page.locator('.cm-content').first()).toContainText('remote_change');
  await expect(page.getByText('Saved · revision 2')).toBeVisible();
});

test('offline save retains edits and recovers after reconnecting', async ({ page, context }) => {
  await ready(page); await saveNew(page, unique());
  await page.locator('.cm-content').first().fill('preserve_me = 99');
  await context.setOffline(true);
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('could not reach the API');
  await expect(page.locator('.cm-content').first()).toContainText('preserve_me');
  await context.setOffline(false);
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByText('Saved · revision 2')).toBeVisible();
});

test('workspace switching changes document scope and creation is persisted', async ({ page, request }) => {
  const name = unique(); await ready(page);
  await page.getByRole('button', { name: 'Create workspace', exact: true }).click();
  await page.getByRole('textbox', { name: 'Workspace name' }).fill(name);
  await page.getByRole('button', { name: 'Create workspace', exact: true }).last().click();
  await expect(page.getByRole('combobox', { name: 'Workspace', exact: true })).toContainText(name);
  await expect(page.getByText('Your saved notebooks and queries will appear here.')).toBeVisible();
  await saveNew(page, `${name}-document`);
  await page.getByRole('combobox', { name: 'Workspace', exact: true }).selectOption('default');
  await expect(page.getByRole('button', { name: `${name}-document Notebook · revision 1` })).toHaveCount(0);
  const workspaces = await (await request.get('/api/v1/workspaces')).json();
  expect(workspaces.items.some((workspace: { name: string }) => workspace.name === name)).toBeTruthy();
});

test('Spark previews use the chosen real API adapter without submitting', async ({ page }) => {
  await ready(page); await page.getByRole('button', { name: 'Compute', exact: true }).click();
  await page.getByRole('tab', { name: 'Manifest preview', exact: true }).click();
  await page.getByRole('textbox', { name: 'Runtime image' }).fill('example.com/team/spark:4.1.3');
  await page.getByRole('textbox', { name: 'Main Python application URI' }).fill('s3://test-lake/jobs/analysis.py');
  await page.getByRole('button', { name: 'Preview manifest', exact: true }).click();
  await expect(page.locator('.code-preview')).toContainText('spark.apache.org/v1');
  await expect(page.locator('.manifest-summary')).toContainText('Not submitted');
  await page.getByRole('combobox', { name: 'Operator', exact: true }).selectOption('kubeflow');
  await page.getByRole('button', { name: 'Preview manifest', exact: true }).click();
  await expect(page.locator('.code-preview')).toContainText('sparkoperator.k8s.io/v1beta2');
  await expect(page.locator('.code-preview')).toContainText('mainApplicationFile');
});

test('broader modules, Sylon limitations and dependency-checked external plans are visible', async ({ page }) => {
  await ready(page); await page.getByRole('button', { name: 'AI & models', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Agent Studio', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Model serving', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Sylon Your AI companion' }).click();
  await expect(page.getByText('Provider not connected', { exact: true })).toBeVisible();
  await expect(page.getByText('An administrator must enable an approved assistant profile', { exact: false })).toBeVisible();
  await page.getByRole('button', { name: 'Close Sylon' }).click();
  await page.getByRole('button', { name: 'Plan deployment', exact: true }).click();
  await page.getByRole('button', { name: 'MLflow', exact: true }).click();
  await expect(page.getByText('3 selected', { exact: true })).toBeVisible();
  await page.getByRole('combobox', { name: 'mlflow ownership' }).selectOption('external');
  await page.getByRole('textbox', { name: 'HTTPS endpoint', exact: true }).fill('https://mlflow.example.test');
  await page.getByRole('button', { name: 'Generate deployment plan', exact: true }).click();
  await expect(page.getByText('Plan validated', { exact: true })).toBeVisible();
  await expect(page.locator('.plan-body')).toContainText('not executable');
  await expect(page.locator('.plan-body')).toContainText('No services have been installed or verified.');
});

test('mobile navigation and editor fit the viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 }); await ready(page);
  await saveNew(page, unique(), 'print("mobile")');
  await page.getByRole('tab', { name: 'Visualize', exact: true }).click();
  const width = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
  expect(width.scroll).toBeLessThanOrEqual(width.client);
  await page.getByRole('button', { name: 'Administration', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'One platform. Your infrastructure.' })).toBeVisible();
});

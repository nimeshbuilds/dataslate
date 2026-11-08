import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { ExecutionBinding, ExecutionRun, Notebook, RunIntent } from '../src/api';

// Documents use the real API. Explicit execution transport fixtures check the UI contract;
// they do not claim that a Spark operator has executed these notebooks.
const binding: ExecutionBinding = { id: 'python-engineering', workspaceId: 'default', name: 'Python engineering', executionKind: 'notebook-python-batch', operator: 'apache', image: `example.test/spark@sha256:${'b'.repeat(64)}`, namespace: 'engineering', serviceAccount: 'engineering-worker', executorInstances: 1, maxRunSeconds: 600 };
const unique = () => `batch-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;

async function save(page: Page, revision: number) {
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByText(`Saved · revision ${revision}`, { exact: true })).toBeVisible();
}
async function newNotebook(page: Page, name: string) {
  await page.goto('/'); await page.getByRole('button', { name: 'Create notebook', exact: true }).click();
  await page.getByRole('textbox', { name: 'Document name' }).fill(name);
  await page.locator('.cm-content').first().fill('answer = 42\nprint(answer)');
}
function runFor(intent: RunIntent): ExecutionRun {
  const at = '2026-11-07T10:00:00Z';
  return { id: 'run-notebook-contract', workspaceId: 'default', bindingId: binding.id, createdAt: at, updatedAt: at, revision: 1, submittedBy: { issuer: 'development', subject: 'development-admin' }, profile: binding,
    state: 'queued', cancelRequested: false, kubernetesName: 'ds-notebook-contract', message: 'Queued for source preparation.', events: [{ at, state: 'queued', message: 'Saved revision accepted.' }], eventsTruncated: false,
    notebook: { ...intent.notebook!, sha256: 'c'.repeat(64), cellIds: ['cell-one'], bytes: 150 }, sourceArtifact: { name: 'ds-notebook-contract-source', state: 'pending' } };
}

test('notebook batch contract: only saved clean Python can submit and the accepted revision links to its run', async ({ page, request }) => {
  const name = unique(); const submissions: RunIntent[] = []; let run: ExecutionRun | undefined;
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => route.fulfill({ json: { items: [binding] } }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => {
    if (route.request().method() === 'POST') { const input = route.request().postDataJSON(); submissions.push(input); run = runFor(input); return route.fulfill({ status: 202, json: run }); }
    return route.fulfill({ json: { items: run ? [run] : [] } });
  });
  await page.route('**/api/v1/workspaces/default/runs/run-notebook-contract', (route) => route.fulfill({ json: run }));
  await newNotebook(page, name);
  await expect(page.getByRole('button', { name: 'Run saved revision as batch', exact: true })).toBeDisabled();
  await save(page, 1);
  await expect(page.getByRole('button', { name: 'Run saved revision as batch', exact: true })).toBeEnabled();
  await page.getByRole('combobox', { name: 'Cell 1 language' }).selectOption('sql'); await save(page, 2);
  await expect(page.getByRole('button', { name: 'Run saved revision as batch', exact: true })).toBeDisabled();
  await expect(page.getByText('Batch execution supports Python-only notebooks.', { exact: false })).toBeVisible();
  await page.getByRole('combobox', { name: 'Cell 1 language' }).selectOption('python');
  await expect(page.getByRole('button', { name: 'Run saved revision as batch', exact: true })).toBeDisabled();
  await save(page, 3);
  const notebooks = await (await request.get('/api/v1/workspaces/default/notebooks')).json();
  const document = notebooks.items.find((item: Notebook) => item.name === name);
  await page.getByRole('button', { name: 'Run saved revision as batch', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Review saved Python batch' });
  await expect(dialog.getByText('Saved revision 3', { exact: true })).toBeVisible();
  await expect(dialog.getByText('engineering-worker', { exact: true })).toBeVisible();
  await expect(dialog.getByText('This binding runs arbitrary Python', { exact: false })).toBeVisible();
  await page.getByRole('button', { name: 'Submit saved revision', exact: true }).click();
  await expect(page.getByText('Run accepted', { exact: true })).toBeVisible();
  expect(submissions).toHaveLength(1); expect(submissions[0].notebook).toEqual({ id: document.id, revision: 3 });
  expect(submissions[0]).not.toHaveProperty('source');
  await page.locator('.cm-content').first().fill('later_edit = True'); await save(page, 4);
  await page.getByRole('button', { name: 'View batch run', exact: true }).click();
  const details = page.getByRole('region', { name: 'Run details' });
  await expect(details.getByRole('heading', { name: 'Saved notebook snapshot' })).toBeVisible();
  await expect(details.locator('dt:has-text("Saved revision") + dd')).toHaveText('3');
  await expect(details.getByText('c'.repeat(64), { exact: true })).toBeVisible();
  await expect(details.getByText('Cell outputs are not available for batch runs.', { exact: false })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Submit application', exact: true })).toBeDisabled(); // Engineering bindings cannot launch as fixed-entrypoint jobs.
});

test('notebook batch contract: a lost response recovers the original revision after edits and reload', async ({ page }) => {
  const name = unique(); const submissions: RunIntent[] = []; let run: ExecutionRun | undefined;
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => route.fulfill({ json: { items: [binding] } }));
  await page.route('**/api/v1/workspaces/default/runs', async (route) => {
    if (route.request().method() !== 'POST') return route.fulfill({ json: { items: run ? [run] : [] } });
    const input = route.request().postDataJSON(); submissions.push(input); run ??= runFor(input);
    if (submissions.length === 1) return route.abort('connectionreset');
    return route.fulfill({ json: run });
  });
  await newNotebook(page, name); await save(page, 1);
  await page.getByRole('button', { name: 'Run saved revision as batch', exact: true }).click();
  await page.getByRole('button', { name: 'Submit saved revision', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('could not reach the API');
  await page.getByRole('button', { name: 'Close', exact: true }).click();
  const key = await page.locator('.notebook-batch .run-key').textContent();
  await page.locator('.cm-content').first().fill('new_saved_code = 99'); await save(page, 2);
  page.once('dialog', (dialog) => dialog.accept()); await page.reload();
  await page.getByRole('button', { name: `${name} Notebook · revision 2`, exact: true }).click();
  await expect(page.locator('.notebook-batch .run-key')).toHaveText(key!);
  expect(submissions).toHaveLength(1);
  await page.locator('.cm-content').first().fill('unsaved_code_must_not_run = True');
  await page.getByRole('button', { name: 'Recover batch submission', exact: true }).click();
  await expect(page.getByText('Recovery refers to revision 1.', { exact: false })).toBeVisible();
  await page.getByRole('button', { name: 'Check batch submission', exact: true }).click();
  await expect(page.getByText('Run accepted', { exact: true })).toBeVisible();
  expect(submissions).toHaveLength(2); expect(submissions[1]).toEqual(submissions[0]); expect(submissions[1].notebook?.revision).toBe(1);
  await expect(page.locator('.cm-content').first()).toContainText('unsaved_code_must_not_run');
  expect(await page.evaluate(() => Object.keys(sessionStorage).filter((name) => name.startsWith('dataslate.run.intent.')))).toEqual([]);
});

test('notebook batch contract: stale saved revision requires explicit reload before a new submission', async ({ page, request }) => {
  const name = unique(); const submissions: RunIntent[] = [];
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => route.fulfill({ json: { items: [binding] } }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => {
    if (route.request().method() !== 'POST') return route.fulfill({ json: { items: [] } });
    const input = route.request().postDataJSON(); submissions.push(input);
    return submissions.length === 1 ? route.fulfill({ status: 409, json: { error: { code: 'notebook_revision_conflict', message: 'The selected notebook revision is stale.' } } }) : route.fulfill({ json: runFor(input) });
  });
  await newNotebook(page, name); await save(page, 1);
  const notebooks = await (await request.get('/api/v1/workspaces/default/notebooks')).json();
  const document: Notebook = notebooks.items.find((item: Notebook) => item.name === name);
  const update = await request.put(`/api/v1/workspaces/default/notebooks/${document.id}`, { data: { name, kind: 'notebook', revision: 1, cells: [{ ...document.cells[0], source: 'remote_revision = 2' }] } });
  expect(update.ok()).toBeTruthy();
  await page.getByRole('button', { name: 'Run saved revision as batch', exact: true }).click();
  await page.getByRole('button', { name: 'Submit saved revision', exact: true }).click();
  await expect(page.getByRole('dialog').getByRole('alert').last()).toContainText('selected notebook revision is stale');
  await expect(page.getByRole('button', { name: 'Submit saved revision', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.getByText('Run accepted', { exact: true })).toHaveCount(0);
  expect(await page.evaluate(() => Object.keys(sessionStorage).filter((name) => name.startsWith('dataslate.run.intent.')))).toEqual([]);
  page.once('dialog', (dialog) => dialog.accept()); await page.getByRole('button', { name: 'Reload saved revision', exact: true }).click();
  await expect(page.locator('.cm-content').first()).toContainText('remote_revision = 2');
  await page.getByRole('button', { name: 'Run saved revision as batch', exact: true }).click();
  await page.getByRole('button', { name: 'Submit saved revision', exact: true }).click();
  await expect(page.getByText('Run accepted', { exact: true })).toBeVisible();
  expect(submissions[0].notebook?.revision).toBe(1); expect(submissions[1].notebook?.revision).toBe(2);
  expect(submissions[0].idempotencyKey).not.toBe(submissions[1].idempotencyKey);
});

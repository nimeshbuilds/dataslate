import { expect, test } from '@playwright/test';
import type { Page, Route } from '@playwright/test';
import type { ExecutionBinding, ExecutionRun, RunIntent } from '../src/api';

async function openRuns(page: Page) {
  await page.goto('/');
  await expect(page.getByRole('button', { name: 'Create notebook', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Compute', exact: true }).click();
  await expect(page.getByRole('tab', { name: 'Application runs', exact: true })).toHaveAttribute('aria-selected', 'true');
}

// Explicit transport fixtures exercise browser recovery/authorization UX. They are not
// a Spark engine and provide no evidence of live Kubernetes execution. Other workspace
// routes and the unavailable-execution test below use the real Go API.
const binding: ExecutionBinding = {
  id: 'approved-analysis', name: 'Approved analysis', workspaceId: 'default', operator: 'apache',
  image: `example.test/spark@sha256:${'a'.repeat(64)}`, mainApplicationFile: 'local:///opt/spark/jobs/analysis.py',
  namespace: 'team-data', serviceAccount: 'analysis-worker', executorInstances: 1,
};
function sampleRun(): ExecutionRun {
  const at = '2026-10-01T10:00:00Z';
  return { id: 'run-browser-contract', workspaceId: 'default', bindingId: binding.id, createdAt: at, updatedAt: at, observedAt: at,
    revision: 1, submittedBy: { issuer: 'development', subject: 'development-admin' }, profile: binding,
    state: 'running', cancelRequested: false, eventsTruncated: false, kubernetesName: 'ds-browser-contract', message: 'Driver is running.',
    events: [{ at, state: 'running', message: 'Application observed by the controller.' }] };
}
async function json(route: Route, body: unknown, status = 200) { await route.fulfill({ status, json: body }); }

test('transport contract: terminal notebook runs continue observing source cleanup', async ({ page }) => {
  let run: ExecutionRun = {
    ...sampleRun(), state: 'succeeded', message: 'Spark completed; source cleanup is pending.',
    profile: { ...binding, executionKind: 'notebook-python-batch', mainApplicationFile: undefined },
    notebook: { id: 'nb-cleanup', revision: 1, sha256: 'd'.repeat(64), cellIds: ['cell-one'], bytes: 100 },
    sourceArtifact: { name: 'ds-browser-contract-source', uid: 'source-uid', state: 'cleanup_pending' },
  };
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => json(route, { items: [run] }));
  // Distinguish the fetched detail from the optimistic list row, so fake time
  // advances only after the initial fetch has installed its next poll timer.
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => json(route, { ...run, revision: run.revision + 1 }));
  await openRuns(page);
  await page.clock.install();
  await page.locator('.run-row').first().click();
  const artifact = page.getByRole('region', { name: 'Run details' }).locator('dt:has-text("Source artifact") + dd');
  await expect(artifact).toHaveText('cleanup pending');
  await expect(page.getByRole('region', { name: 'Run details' }).locator('dt:text-is("Revision") + dd')).toHaveText('2');
  run = { ...run, revision: 2, sourceArtifact: { ...run.sourceArtifact!, state: 'deleted' } };
  await page.clock.fastForward(5500);
  await expect(artifact).toHaveText('deleted');
});

test('real API keeps execution disabled without an approved identity binding', async ({ page, request }) => {
  await openRuns(page);
  await expect(page.getByRole('heading', { name: 'No approved execution bindings' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Submit application', exact: true })).toBeDisabled();
  await expect(page.getByRole('heading', { name: 'No application runs yet' })).toBeVisible();
  const response = await request.get('/api/v1/workspaces/default/execution-bindings');
  expect(response.status()).toBe(200); expect((await response.json()).items).toEqual([]);
  const rejected = await request.post('/api/v1/workspaces/default/runs', { data: { bindingId: 'not-approved', idempotencyKey: 'browser-disabled-check' } });
  expect([403, 404, 501]).toContain(rejected.status());
  expect((await rejected.json()).error.code).toBeTruthy();
  await page.getByRole('button', { name: 'Develop', exact: true }).click();
  await page.getByRole('button', { name: 'Create notebook', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Run all', exact: true })).toBeDisabled();
});

test('transport contract: a lost submit response reuses one intent and cancellation waits for observation', async ({ page }) => {
  let run: ExecutionRun | undefined;
  const requests: RunIntent[] = [];
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', async (route) => {
    if (route.request().method() === 'POST') {
      requests.push(route.request().postDataJSON()); run ??= sampleRun();
      if (requests.length === 1) await route.abort('connectionreset');
      else await json(route, run);
    } else await json(route, { items: run ? [run] : [] });
  });
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => json(route, run));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract/logs', (route) => json(route, { error: { code: 'capability_unavailable', message: 'Driver log retrieval is not configured.' } }, 501));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract/cancel', async (route) => {
    run = { ...run!, state: 'cancel_requested', cancelRequested: true, revision: 2, message: 'Cancellation is awaiting reconciliation.' };
    await json(route, run, 202);
  });
  await openRuns(page);
  await page.getByRole('button', { name: 'Submit application', exact: true }).click();
  await expect(page.getByText('Submission outcome is unresolved.', { exact: true })).toBeVisible();
  await expect(page.getByRole('combobox', { name: 'Execution binding' })).toBeDisabled();
  const key = await page.locator('.run-key').textContent();
  await page.getByRole('tab', { name: 'Manifest preview', exact: true }).click();
  await page.getByRole('tab', { name: 'Application runs', exact: true }).click();
  await expect(page.locator('.run-key')).toHaveText(key!);
  page.once('dialog', (dialog) => dialog.accept());
  await page.reload();
  await page.getByRole('button', { name: 'Compute', exact: true }).click();
  await expect(page.locator('.run-key')).toHaveText(key!);
  expect(requests).toHaveLength(1); // Reload only restores intent; it must never resend automatically.
  await page.getByRole('button', { name: 'Check submission', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Run details' })).toBeVisible();
  expect(requests).toHaveLength(2); expect(requests[0]).toEqual(requests[1]); expect(requests[0].idempotencyKey).toBe(key);
  await expect(page.getByText('Submission outcome is unresolved.', { exact: true })).toHaveCount(0);
  expect(await page.evaluate(() => Object.keys(sessionStorage).filter((key) => key.startsWith('dataslate.run.intent.')))).toEqual([]);
  await expect(page.getByText('Backend observed', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Load logs', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Driver log retrieval is not configured.');
  await page.getByRole('button', { name: 'Cancel run', exact: true }).click();
  await expect(page.getByRole('dialog', { name: 'Cancel this application?' })).toBeVisible();
  await page.getByRole('button', { name: 'Keep running', exact: true }).click();
  expect(run!.state).toBe('running');
  await page.getByRole('button', { name: 'Cancel run', exact: true }).click();
  await page.getByRole('button', { name: 'Request cancellation', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Cancellation requested', exact: true })).toBeDisabled();
  await expect(page.getByText('The workload is not confirmed stopped until the controller observes cancellation.', { exact: false })).toBeVisible();
  expect(run!.state).toBe('cancel_requested');
  run = { ...run!, state: 'cancelled', revision: 3, message: 'Application absence observed.' };
  await page.getByRole('button', { name: 'Refresh runs', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Run details' }).getByText('cancelled', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Cancellation requested', exact: true })).toHaveCount(0);
  await expect(page.getByText('The workload is not confirmed stopped until the controller observes cancellation.', { exact: false })).toHaveCount(0);
});

test('transport contract: workspace switching clears run details, profiles and logs', async ({ page, request }) => {
  const run = { ...sampleRun(), eventsTruncated: true };
  const name = `run-scope-${Date.now()}`;
  const created = await request.post('/api/v1/workspaces', { data: { name, slug: name } });
  expect(created.ok()).toBeTruthy(); const workspace = await created.json();
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => json(route, { items: [run] }));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => json(route, run));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract/logs', (route) => json(route, { text: 'restricted-run-content', truncated: true }));
  await openRuns(page);
  await page.locator('.run-row').click();
  await page.getByRole('button', { name: 'Load logs', exact: true }).click();
  await expect(page.locator('.run-logs')).toContainText('restricted-run-content');
  await expect(page.getByText('Earlier events were omitted.', { exact: false })).toBeVisible();
  await expect(page.getByText('This response is truncated.', { exact: false })).toBeVisible();
  await page.getByRole('combobox', { name: 'Workspace', exact: true }).selectOption(workspace.id);
  await expect(page.getByRole('heading', { name: 'No approved execution bindings' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Run details' })).toHaveCount(0);
  await expect(page.getByText('restricted-run-content')).toHaveCount(0);
  await expect(page.getByText('analysis-worker', { exact: true })).toHaveCount(0);
  const width = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
  expect(width.scroll).toBeLessThanOrEqual(width.client);
});

test('transport contract: revoked access clears previously fetched execution data', async ({ page }) => {
  let revoked = false; const run = sampleRun();
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => revoked ? json(route, { error: { code: 'forbidden', message: 'Workspace access was revoked.' } }, 403) : json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => revoked ? json(route, { error: { code: 'forbidden', message: 'Workspace access was revoked.' } }, 403) : json(route, { items: [run] }));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => revoked ? json(route, { error: { code: 'forbidden', message: 'Workspace access was revoked.' } }, 403) : json(route, run));
  await openRuns(page); await page.locator('.run-row').click();
  await expect(page.getByRole('region', { name: 'Run details' })).toBeVisible();
  revoked = true; await page.getByRole('button', { name: 'Refresh runs', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Workspace access was revoked.');
  await expect(page.getByRole('region', { name: 'Run details' })).toHaveCount(0);
  await expect(page.locator('.run-row')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Submit application', exact: true })).toBeDisabled();
});

test('transport contract: mobile execution profile and history fit the viewport', async ({ page }) => {
  const run = sampleRun();
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => json(route, { items: [run] }));
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => json(route, run));
  await page.setViewportSize({ width: 390, height: 844 }); await openRuns(page);
  await page.locator('.run-row').click();
  await expect(page.getByRole('region', { name: 'Run details' })).toBeVisible();
  const width = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, client: document.documentElement.clientWidth }));
  expect(width.scroll).toBeLessThanOrEqual(width.client);
});

test('transport contract: execution polling stops when leaving the compute view', async ({ page }) => {
  const run = sampleRun(); let reads = 0;
  await page.clock.install();
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => { reads += 1; return json(route, { items: [run] }); });
  await openRuns(page); await expect(page.locator('.run-row')).toHaveCount(1);
  await page.getByRole('button', { name: 'Develop', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Create notebook', exact: true })).toBeVisible();
  const previous = reads;
  await page.clock.runFor(20000);
  expect(reads).toBe(previous);
});

test('transport contract: a rejected capacity request stays explicit and is not retried', async ({ page }) => {
  let submissions = 0;
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => {
    if (route.request().method() !== 'POST') return json(route, { items: [] });
    submissions += 1;
    return json(route, { error: { code: 'run_capacity_exceeded', message: 'Workspace active-run capacity has been reached.' } }, 429);
  });
  await openRuns(page);
  await page.getByRole('button', { name: 'Submit application', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Workspace active-run capacity has been reached.');
  await expect(page.getByRole('combobox', { name: 'Execution binding' })).toBeEnabled();
  await expect(page.getByRole('button', { name: 'Check submission', exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'Refresh runs', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'No application runs yet' })).toBeVisible();
  expect(submissions).toBe(1);
});


test('transport contract: unavailable recovery storage prevents sending a new job', async ({ page }) => {
  let submissions = 0;
  await page.addInitScript(() => {
    const write = Storage.prototype.setItem;
    Storage.prototype.setItem = function (key, value) {
      if (key.startsWith('dataslate.run.intent.')) throw new DOMException('Storage unavailable', 'QuotaExceededError');
      return write.call(this, key, value);
    };
  });
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', (route) => {
    if (route.request().method() === 'POST') submissions += 1;
    return json(route, { items: [] });
  });
  await openRuns(page);
  await page.getByRole('button', { name: 'Submit application', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('No request was sent.');
  expect(submissions).toBe(0);
});

test('transport contract: incomplete or wrong-owner receipts cannot clear a durable submission', async ({ page }) => {
  const run = sampleRun(); const requests: RunIntent[] = [];
  await page.route('**/api/v1/workspaces/default/execution-bindings', (route) => json(route, { items: [binding] }));
  await page.route('**/api/v1/workspaces/default/runs', async (route) => {
    if (route.request().method() !== 'POST') return json(route, { items: [] });
    requests.push(route.request().postDataJSON());
    if (requests.length === 1) return json(route, { id: run.id, workspaceId: run.workspaceId, bindingId: run.bindingId });
    if (requests.length === 2) return json(route, { ...run, submittedBy: { ...run.submittedBy, subject: 'another-user' }, message: 'other-user-private-message' });
    return json(route, run);
  });
  await page.route('**/api/v1/workspaces/default/runs/run-browser-contract', (route) => json(route, run));
  await openRuns(page);
  await page.getByRole('button', { name: 'Submit application', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('invalid run confirmation');
  const key = await page.locator('.run-key').textContent();
  await expect(page.getByRole('region', { name: 'Run details' })).toHaveCount(0);
  await page.getByRole('button', { name: 'Check submission', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('invalid run confirmation');
  await expect(page.locator('.run-key')).toHaveText(key!);
  await expect(page.getByText('other-user-private-message')).toHaveCount(0);
  expect(await page.evaluate(() => Object.keys(sessionStorage).filter((name) => name.startsWith('dataslate.run.intent.')).length)).toBe(1);
  await page.getByRole('button', { name: 'Check submission', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Run details' })).toBeVisible();
  expect(requests).toHaveLength(3);
  expect(requests[1]).toEqual(requests[0]); expect(requests[2]).toEqual(requests[0]);
  expect(await page.evaluate(() => Object.keys(sessionStorage).filter((name) => name.startsWith('dataslate.run.intent.')))).toEqual([]);
});

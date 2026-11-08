import { test, expect } from '@playwright/test';
import type { Page } from '@playwright/test';
import type { WorkflowBinding, WorkflowDefinition, WorkflowHistory, WorkflowIntent, WorkflowRun } from '../src/workflow-api';

// Airflow transport fixtures test browser contracts and failure handling. They do not run a scheduler.
const ws = 'default'; const now = '2026-11-08T16:00:00Z'; const owner = { issuer: 'development', subject: 'development-admin' };
const binding: WorkflowBinding = { id: 'orders-workflow', workspaceId: ws, name: 'Daily orders', policyVersion: 'workflow-v1', dagId: 'orders_lakehouse', dagVersion: 7, bundleVersion: '9fb829aaa9176e28325f6d72f63021f01146d2948', airflowVersion: '3.4.0', maxRunSeconds: 3600, scheduler: 'airflow', retryOwner: 'airflow' };
const definition: WorkflowDefinition = { bindingId: binding.id, workspaceId: ws, policyVersion: binding.policyVersion, dagId: binding.dagId, dagVersion: binding.dagVersion, bundleVersion: binding.bundleVersion, observedAt: now, paused: false, stale: false, hasImportErrors: false, scheduleSummary: 'At 06:00 every day', timezone: 'America/Chicago', nextRunAt: '2026-11-09T12:00:00Z', versionMatches: true, canTrigger: true, reasonCode: 'ready', tasks: [{ id: 'ingest_orders', downstreamTaskIds: ['validate_orders'] }, { id: 'validate_orders', downstreamTaskIds: ['publish_orders'] }, { id: 'publish_orders', downstreamTaskIds: [] }] };
const run: WorkflowRun = { id: 'workflow-run-one', workspaceId: ws, createdAt: now, updatedAt: now, revision: 1, bindingId: binding.id, policyVersion: binding.policyVersion, dagId: binding.dagId, dagVersion: binding.dagVersion, bundleVersion: binding.bundleVersion, backendRunId: 'dataslate__workflow-run-one', submittedBy: owner, state: 'running', dispatchStarted: true, reconciliationComplete: false, message: 'Airflow observed this workflow running.', observedAt: now, startedAt: now, tasks: [{ id: 'ingest_orders', mapIndex: -1, tryNumber: 1, state: 'success', startedAt: now, endedAt: now }, { id: 'validate_orders', mapIndex: -1, tryNumber: 1, state: 'running', startedAt: now }, { id: 'publish_orders', mapIndex: -1, tryNumber: 0, state: 'pending' }], tasksTruncated: false };
const history: WorkflowHistory = { observedAt: now, truncated: false, items: [{ dagId: binding.dagId, backendRunId: 'scheduled__2026-11-08T12:00:00+00:00', state: 'success', runType: 'scheduled', dagVersion: 6, bundleVersion: 'previous-approved-bundle', logicalDate: '2026-11-08T12:00:00Z', startedAt: now, endedAt: now }] };
const recoveryKey = `dataslate.workflow.intent.${JSON.stringify([owner.issuer, owner.subject, ws])}`;
async function fixture(page: Page) {
  const state = { bindings: [structuredClone(binding)], definition: structuredClone(definition), run: structuredClone(run), history: structuredClone(history), inputs: [] as WorkflowIntent[], submitted: false, revoked: false, runReads: 0, definitionReads: 0 };
  const denied = { status: 403, json: { error: { code: 'forbidden', message: 'Workflow access was revoked.' } } };
  await page.route(`**/api/v1/workspaces/${ws}/workflow-bindings`, (r) => r.fulfill({ json: { items: state.revoked ? [] : state.bindings } }));
  await page.route(`**/api/v1/workspaces/${ws}/workflow-bindings/${binding.id}/definition`, (r) => { state.definitionReads++; return r.fulfill(state.revoked ? denied : { json: state.definition }); });
  await page.route(`**/api/v1/workspaces/${ws}/workflow-bindings/${binding.id}/history`, (r) => r.fulfill(state.revoked ? denied : { json: state.history }));
  await page.route(`**/api/v1/workspaces/${ws}/workflow-runs`, (r) => { if (state.revoked) return r.fulfill(denied); if (r.request().method() === 'POST') { state.inputs.push(r.request().postDataJSON()); state.submitted = true; return r.fulfill({ status: 202, json: { run: state.run, replayed: false } }); } return r.fulfill({ json: { items: state.submitted ? [state.run] : [] } }); });
  await page.route(`**/api/v1/workspaces/${ws}/workflow-runs/${run.id}`, (r) => { state.runReads++; return r.fulfill(state.revoked ? denied : { json: state.run }); });
  return state;
}
async function open(page: Page) { await page.goto('/'); await page.getByRole('button', { name: 'Pipelines', exact: true }).click(); await page.getByRole('tab', { name: 'Workflows', exact: true }).click(); }
async function submit(page: Page) { await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click(); await expect(page.getByRole('dialog')).toHaveCount(0); await expect(page.getByRole('region', { name: 'Workflow run details' })).toBeVisible(); }

test('observes approved schedule and graph, then explicitly triggers the exact workflow', async ({ page }) => {
  const state = await fixture(page); await page.clock.install(); await open(page);
  const dag = page.getByRole('region', { name: 'Workflow definition', exact: true }); await expect(dag).toContainText('America/Chicago'); await expect(dag).toContainText('At 06:00 every day'); await expect(page.getByRole('region', { name: 'Deployed task graph', exact: true })).toContainText('publish_orders'); await expect(page.getByRole('img', { name: '3 tasks and their deployed dependencies' })).toBeVisible(); await expect(page.getByRole('region', { name: 'Recent Airflow runs', exact: true })).toContainText('scheduled__2026-11-08'); expect(state.inputs).toHaveLength(0);
  const displayedZone = await page.evaluate((instant) => new Intl.DateTimeFormat(undefined, { timeZoneName: 'short' }).formatToParts(new Date(instant)).find((part) => part.type === 'timeZoneName')?.value, definition.nextRunAt!);
  expect(displayedZone).toBeTruthy(); await expect(dag).toContainText(displayedZone!);
  await submit(page); expect(state.inputs).toHaveLength(1); expect(Object.keys(state.inputs[0]).sort()).toEqual(['bindingId', 'bundleVersion', 'dagId', 'dagVersion', 'idempotencyKey', 'policyVersion']); expect(state.inputs[0]).toMatchObject({ bindingId: binding.id, policyVersion: binding.policyVersion, dagId: binding.dagId, dagVersion: binding.dagVersion, bundleVersion: binding.bundleVersion }); expect(state.definitionReads).toBeGreaterThanOrEqual(2); await expect(page.getByRole('region', { name: 'Observed workflow tasks' })).toContainText('running');
  state.run.state = 'succeeded'; state.run.reconciliationComplete = true; state.run.revision++; state.run.endedAt = now; state.run.message = 'Airflow confirmed this DAG run succeeded.'; await page.clock.fastForward(2001); await expect(page.getByRole('region', { name: 'Workflow run details' })).toContainText('Airflow confirmed this DAG run succeeded.'); expect(state.inputs).toHaveLength(1); expect(await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey)).toBeNull(); await page.evaluate(() => window.scrollTo(0, 0)); await page.screenshot({ path: '/tmp/dataslate-workflows-desktop.png', fullPage: true });
});

for (const reason of ['paused', 'stale', 'import_errors', 'version_mismatch'] as const) test(`${reason} DAG cannot be triggered`, async ({ page }) => {
  const state = await fixture(page); state.definition.canTrigger = false; state.definition.reasonCode = reason;
  if (reason === 'paused') state.definition.paused = true; if (reason === 'stale') state.definition.stale = true; if (reason === 'import_errors') state.definition.hasImportErrors = true; if (reason === 'version_mismatch') state.definition.versionMatches = false;
  await open(page); await expect(page.getByRole('button', { name: 'Review manual run', exact: true })).toBeDisabled(); await expect(page.getByRole('region', { name: 'Workflow definition', exact: true })).toContainText(reason); expect(state.inputs).toHaveLength(0);
});

test('lost trigger response recovers the same key after reload without a second operation', async ({ page }) => {
  const state = await fixture(page); let calls = 0;
  await page.route('**/workflow-runs', (r) => { if (r.request().method() !== 'POST') return r.fallback(); state.inputs.push(r.request().postDataJSON()); state.submitted = true; calls++; return calls === 1 ? r.abort('connectionreset') : r.fulfill({ status: 202, json: { run: state.run, replayed: true } }); });
  await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click(); await expect(page.getByRole('dialog').getByRole('alert')).toContainText('could not reach'); await page.getByRole('button', { name: 'Close', exact: true }).click(); page.once('dialog', (d) => d.accept()); await open(page); await page.getByRole('button', { name: 'Recover workflow submission', exact: true }).click(); await page.getByRole('button', { name: 'Check workflow submission', exact: true }).click(); await expect(page.getByRole('dialog')).toHaveCount(0); expect(state.inputs).toHaveLength(2); expect(state.inputs[1]).toEqual(state.inputs[0]); expect(await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey)).toBeNull();
});

test('binding replacement after review is rejected without starting its new DAG or replacing recovery', async ({ page }) => {
  const state = await fixture(page); await page.clock.install(); let accepted = 0;
  await page.route('**/workflow-runs', (r) => {
    if (r.request().method() !== 'POST') return r.fallback();
    const input: WorkflowIntent = r.request().postDataJSON(); state.inputs.push(input); const current = state.bindings[0];
    if (input.policyVersion !== current.policyVersion || input.dagId !== current.dagId || input.dagVersion !== current.dagVersion || input.bundleVersion !== current.bundleVersion) return r.fulfill({ status: 409, json: { error: { code: 'workflow_approval_changed', message: 'The reviewed workflow approval has changed.' } } });
    accepted++; return r.fulfill({ status: 202, json: { run: state.run, replayed: false } });
  });
  await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click();
  state.bindings[0] = { ...binding, policyVersion: 'workflow-v2', dagVersion: 8, bundleVersion: 'replacement-bundle' };
  state.definition = { ...definition, policyVersion: 'workflow-v2', dagVersion: 8, bundleVersion: 'replacement-bundle' };
  await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click();
  await expect(page.getByRole('dialog').getByRole('alert')).toContainText('approval has changed');
  expect(accepted).toBe(0); expect(state.inputs).toHaveLength(1);
  const original = state.inputs[0]; expect(original).toMatchObject({ policyVersion: binding.policyVersion, dagId: binding.dagId, dagVersion: binding.dagVersion, bundleVersion: binding.bundleVersion });
  expect(JSON.parse((await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey))!)).toEqual(original);
  await page.getByRole('button', { name: 'Close', exact: true }).click(); await expect(page.getByRole('button', { name: 'Review manual run', exact: true })).toBeDisabled();
  page.once('dialog', (d) => d.accept()); await open(page);
  await page.getByRole('button', { name: 'Recover workflow submission', exact: true }).click();
  await expect(page.getByRole('dialog')).toContainText(binding.bundleVersion); await expect(page.getByRole('dialog')).toContainText(binding.policyVersion);
  await expect(page.getByRole('dialog')).not.toContainText('replacement-bundle');
  await page.getByRole('button', { name: 'Check workflow submission', exact: true }).click();
  await expect(page.getByRole('dialog').getByRole('alert')).toContainText('approval has changed');
  expect(state.inputs).toEqual([original, original]); expect(accepted).toBe(0); await expect(page.getByRole('region', { name: 'Workflow run details' })).toHaveCount(0);
  expect(JSON.parse((await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey))!)).toEqual(original);
});

test('recovery rejects a receipt for the replacement approval even when it is currently granted', async ({ page }) => {
  const state = await fixture(page); let calls = 0;
  await page.route('**/workflow-runs', (r) => { if (r.request().method() !== 'POST') return r.fallback(); state.inputs.push(r.request().postDataJSON()); calls++; return calls === 1 ? r.abort('connectionreset') : r.fulfill({ status: 202, json: { run: state.run, replayed: true } }); });
  await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click(); await expect(page.getByRole('dialog').getByRole('alert')).toContainText('could not reach');
  await page.getByRole('button', { name: 'Close', exact: true }).click();
  state.bindings[0] = { ...binding, policyVersion: 'workflow-v2', dagVersion: 8, bundleVersion: 'replacement-bundle' };
  state.definition = { ...definition, policyVersion: 'workflow-v2', dagVersion: 8, bundleVersion: 'replacement-bundle' };
  state.run = { ...run, policyVersion: 'workflow-v2', dagVersion: 8, bundleVersion: 'replacement-bundle' };
  page.once('dialog', (d) => d.accept()); await open(page); await page.getByRole('button', { name: 'Recover workflow submission', exact: true }).click(); await page.getByRole('button', { name: 'Check workflow submission', exact: true }).click();
  await expect(page.getByRole('dialog').getByRole('alert')).toContainText('does not match');
  expect(state.inputs).toHaveLength(2); expect(state.inputs[1]).toEqual(state.inputs[0]); expect(JSON.parse((await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey))!)).toEqual(state.inputs[0]);
  await expect(page.getByRole('region', { name: 'Workflow run details' })).toHaveCount(0);
});

test('an old recovery record without its reviewed approval fails closed before dispatch', async ({ page }) => {
  const state = await fixture(page); await page.goto('/');
  const old = { bindingId: binding.id, idempotencyKey: 'prior-request-key' };
  await page.evaluate(({ key, value }) => sessionStorage.setItem(key, JSON.stringify(value)), { key: recoveryKey, value: old });
  await open(page); await expect(page.getByRole('alert')).toContainText('recovery is unavailable or invalid'); await expect(page.getByRole('button', { name: 'Review manual run', exact: true })).toBeDisabled(); expect(state.inputs).toHaveLength(0);
  expect(JSON.parse((await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey))!)).toEqual(old);
});

for (const mismatch of ['owner', 'binding', 'version', 'final-state', 'protected-conf'] as const) test(`rejects ${mismatch} trigger receipt while retaining exact recovery`, async ({ page }) => {
  await fixture(page); const bad = structuredClone(run) as WorkflowRun & { conf?: unknown }; if (mismatch === 'owner') bad.submittedBy.subject = 'another-user'; if (mismatch === 'binding') bad.bindingId = 'another-binding'; if (mismatch === 'version') bad.dagVersion++; if (mismatch === 'final-state') bad.reconciliationComplete = true; if (mismatch === 'protected-conf') bad.conf = { private: 'should-not-render' };
  await page.route('**/workflow-runs', (r) => r.request().method() === 'POST' ? r.fulfill({ status: 202, json: { run: bad, replayed: false } }) : r.fallback()); await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click(); await expect(page.getByRole('dialog').getByRole('alert')).toContainText('does not match'); expect(await page.evaluate((key) => sessionStorage.getItem(key), recoveryKey)).not.toBeNull(); await expect(page.getByRole('region', { name: 'Workflow run details' })).toHaveCount(0); await expect(page.locator('body')).not.toContainText('should-not-render');
});

test('storage failure prevents dispatch', async ({ page }) => {
  const state = await fixture(page); await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); await page.evaluate(() => { const original = Storage.prototype.setItem; Storage.prototype.setItem = function(k, v) { if (k.startsWith('dataslate.workflow.intent.')) throw new Error('Unavailable'); return original.call(this, k, v); }; }); await page.getByRole('button', { name: 'Submit reviewed workflow', exact: true }).click(); await expect(page.getByRole('dialog').getByRole('alert')).toContainText('No workflow request was sent'); expect(state.inputs).toHaveLength(0);
});

test('unknown dispatch is observed without retry until reconciliation finishes', async ({ page }) => {
  const state = await fixture(page); state.run.state = 'unknown'; state.run.message = 'Awaiting exact upstream readback.'; await page.clock.install(); await open(page); await submit(page); await expect(page.getByRole('region', { name: 'Workflow run details' })).toContainText('reading the same Airflow run'); const reads = state.runReads; state.run.message = 'Readback remains uncertain.'; await page.clock.fastForward(2001); await expect(page.getByRole('region', { name: 'Workflow run details' })).toContainText('Readback remains uncertain.'); expect(state.runReads).toBeGreaterThan(reads); state.run.reconciliationComplete = true; state.run.revision++; await page.clock.fastForward(2001); await expect(page.getByRole('region', { name: 'Workflow run details' })).toContainText('Airflow run may still be active'); const stopped = state.runReads; await page.clock.fastForward(12000); expect(state.runReads).toBe(stopped); expect(state.inputs).toHaveLength(1);
});

test('current grant revocation clears definition, schedule and owned run details', async ({ page }) => {
  const state = await fixture(page); await page.clock.install(); await open(page); await submit(page); state.revoked = true; await page.clock.fastForward(10001); await expect(page.getByRole('region', { name: 'Workflow run details' })).toHaveCount(0); await expect(page.getByRole('region', { name: 'Workflow definition', exact: true })).toHaveCount(0); await expect(page.locator('body')).not.toContainText(binding.bundleVersion); await expect(page.getByRole('button', { name: 'Review manual run', exact: true })).toHaveCount(0); expect(state.inputs).toHaveLength(1);
});

test('a changed deployed version during review disables the trigger', async ({ page }) => {
  const state = await fixture(page); await page.clock.install(); await open(page); await page.getByRole('button', { name: 'Review manual run', exact: true }).click(); state.definition.versionMatches = false; state.definition.canTrigger = false; state.definition.reasonCode = 'version_mismatch'; await page.clock.fastForward(10001); await expect(page.getByRole('button', { name: 'Submit reviewed workflow', exact: true })).toBeDisabled(); await expect(page.getByRole('dialog').getByRole('alert')).toContainText('eligibility changed'); expect(state.inputs).toHaveLength(0);
});

test('task truncation cannot imply completion and scheduled history stays distinct from ownership', async ({ page }) => {
  const state = await fixture(page); state.run.tasks = [state.run.tasks[0]]; state.run.tasksTruncated = true; state.history.truncated = true; await open(page); await submit(page); const detail = page.getByRole('region', { name: 'Workflow run details' }); await expect(detail).toContainText('Task observations are incomplete'); await expect(detail).toContainText('running'); await expect(page.getByRole('region', { name: 'Airflow scheduled and manual history' })).toContainText('not assumed to belong'); await expect(page.getByRole('region', { name: 'Airflow scheduled and manual history' })).toContainText('partial history');
});

test('protected upstream metadata fails closed without exposing conf', async ({ page }) => {
  await fixture(page); await page.route('**/workflow-bindings/*/history', (r) => r.fulfill({ json: { ...history, items: [{ ...history.items[0], conf: { sensitive: 'private-history-secret' } }] } })); await open(page); await expect(page.getByRole('region', { name: 'Airflow scheduled and manual history' }).getByRole('alert')).toContainText('Protected configuration'); await expect(page.locator('body')).not.toContainText('private-history-secret');
});

test('workflow definition and details clear on workspace change', async ({ page, request }) => {
  await fixture(page); const slug = `workflow-${Date.now()}`; const other = await (await request.post('/api/v1/workspaces', { data: { name: slug, slug } })).json(); for (const route of ['workflow-bindings', 'workflow-runs']) await page.route(`**/api/v1/workspaces/${other.id}/${route}`, (r) => r.fulfill({ json: { items: [] } })); await open(page); await submit(page); await page.getByRole('combobox', { name: 'Workspace', exact: true }).selectOption(other.id); await expect(page.getByRole('heading', { name: 'No workflows granted' })).toBeVisible(); await expect(page.locator('body')).not.toContainText(binding.bundleVersion); await expect(page.getByRole('region', { name: 'Workflow run details' })).toHaveCount(0);
});

test('pre-dispatch failure does not claim Airflow task failure or cancellation', async ({ page }) => {
  const state = await fixture(page); state.run.state = 'failed'; state.run.dispatchStarted = false; state.run.reconciliationComplete = true; state.run.tasks = []; state.run.message = 'DAG admission failed before dispatch.'; await open(page); await submit(page); await expect(page.getByRole('region', { name: 'Workflow run details' })).toContainText('not a reported Airflow task failure'); await expect(page.getByRole('button', { name: /Cancel.*workflow|Retry.*workflow/ })).toHaveCount(0);
});

test('workflow schedules and task observations fit narrow screens', async ({ page }) => {
  await fixture(page); await page.setViewportSize({ width: 390, height: 844 }); await open(page); await submit(page); await page.evaluate(() => window.scrollTo(0, 0)); await page.screenshot({ path: '/tmp/dataslate-workflows-mobile.png', fullPage: true }); const size = await page.evaluate(() => ({ width: document.documentElement.clientWidth, scroll: document.documentElement.scrollWidth })); expect(size.scroll).toBeLessThanOrEqual(size.width); await expect(page.getByRole('region', { name: 'Observed workflow tasks' })).toBeVisible();
});

test('real unconfigured workflow API returns no invented schedules or executions', async ({ page, request }) => {
  for (const route of ['workflow-bindings', 'workflow-runs']) { const result = await request.get(`/api/v1/workspaces/default/${route}`); expect(result.status()).toBe(200); expect(await result.json()).toEqual({ items: [] }); }
  await open(page); await expect(page.getByRole('heading', { name: 'No workflows granted' })).toBeVisible(); await expect(page.getByRole('button', { name: 'Review manual run' })).toHaveCount(0);
});

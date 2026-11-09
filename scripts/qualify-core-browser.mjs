#!/usr/bin/env node
// Disposable fixture qualification only. Never point this at a customer IdP.
// Tokens/passwords stay in memory; no traces, screenshots or storage-state files.
import { createHash, randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFile, stat, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const APP = 'https://workspace.dataslate.test:18444';
const ISSUER = 'https://identity.dataslate.test:18443/realms/dataslate';
const TOKEN_ENDPOINT = `${ISSUER}/protocol/openid-connect/token`;
const SOURCE = "qualification_marker = 'dataslate-core-browser'\nprint(qualification_marker)";
const ROLES = ['admin', 'editor', 'viewer', 'outsider'];
const checks = [];
let stage = 'arguments';
let browser;

function options(argv) {
  const result = {};
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i];
    if (!['--private-dir', '--output', '--mode', '--previous', '--chrome'].includes(key)
      || !argv[i + 1] || argv[i + 1].startsWith('--') || Object.hasOwn(result, key.slice(2))) {
      throw new Error('invalid_arguments');
    }
    result[key.slice(2)] = argv[i + 1];
  }
  return { mode: 'seed', ...result };
}

function check(name, condition) {
  stage = name;
  if (!condition) throw new Error('qualification_check_failed');
  checks.push({ name, passed: true });
}

async function acceptedResponse(name, response, expected) {
  const status = response?.status() ?? 0;
  if (status !== expected) {
    // Report only known contract categories, never a server message or body.
    const body = response ? await response.json().catch(() => null) : null;
    const code = ['cross_origin_denied', 'forbidden', 'unauthorized', 'invalid_workspace',
      'invalid_notebook', 'conflict', 'storage_unavailable', 'internal_error'].includes(body?.error?.code)
      ? body.error.code : 'unexpected_response';
    stage = `${name}.http_${status}.${code}`;
    throw new Error('qualification_response_failed');
  }
  check(name, true);
}

function jwtClaims(token) {
  if (typeof token !== 'string' || token.length > 32768 || token.split('.').length !== 3) throw new Error('invalid_fixture_token');
  return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
}

async function privateJSON(path) {
  const info = await stat(path);
  if (!info.isFile() || info.size > 65536 || (info.mode & 0o077)) throw new Error('invalid_private_fixture');
  return JSON.parse(await readFile(path, 'utf8'));
}

function certificatePin(privateDir) {
  const cert = join(privateDir, 'server.crt');
  const ca = join(privateDir, 'ca.crt');
  for (const hostname of ['identity.dataslate.test', 'workspace.dataslate.test']) {
    execFileSync('openssl', ['verify', '-CAfile', ca, '-verify_hostname', hostname, cert],
      { timeout: 10000, stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 65536 });
  }
  const publicKey = execFileSync('openssl', ['x509', '-in', cert, '-pubkey', '-noout'],
    { timeout: 10000, stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 65536 });
  const der = execFileSync('openssl', ['pkey', '-pubin', '-outform', 'DER'],
    { input: publicKey, timeout: 10000, stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: 65536 });
  return createHash('sha256').update(der).digest('base64');
}

async function api(page, token, path, method = 'GET', body) {
  return page.evaluate(async ({ token, path, method, body }) => {
    const response = await fetch(path, {
      method, credentials: 'omit', redirect: 'error', signal: AbortSignal.timeout(15000),
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json', ...(body ? { 'Content-Type': 'application/json' } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    const text = await response.text();
    if (text.length > 1048576) throw new Error('response_too_large');
    return { status: response.status, body: text ? JSON.parse(text) : null };
  }, { token, path, method, body });
}

async function noStoredTokens(page, tokens, role) {
  stage = `${role}.storage`;
  const clean = await page.evaluate((secretValues) => {
    for (const storage of [localStorage, sessionStorage]) {
      for (let i = 0; i < storage.length; i++) {
        const key = storage.key(i);
        const value = storage.getItem(key) ?? '';
        if (secretValues.some((secret) => secret && value.includes(secret))
          || /(?:access_token|id_token|refresh_token)\s*["']?\s*:/.test(value)
          || /eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/.test(value)
          || key.startsWith('oidc.user:')) return false;
      }
    }
    return true;
  }, tokens);
  check(`${role}.no_tokens_in_browser_storage`, clean);
}

async function login(role, password) {
  stage = `${role}.browser_login`;
  const context = await browser.newContext(); // TLS bypass and tracing remain disabled.
  context.setDefaultTimeout(30000);
  const page = await context.newPage();
  try {
    await page.goto(APP, { waitUntil: 'domcontentloaded', timeout: 60000 });
    const tokenResponse = page.waitForResponse((response) => response.url() === TOKEN_ENDPOINT
      && response.request().method() === 'POST', { timeout: 60000 }).catch(() => null);
    await page.getByRole('button', { name: 'Continue with your organization' }).click();
    await page.waitForURL((url) => url.origin === new URL(ISSUER).origin
      && url.pathname === '/realms/dataslate/protocol/openid-connect/auth');
    const authorization = new URL(page.url());
    const challenge = authorization.searchParams.get('code_challenge');
    check(`${role}.authorization_code_pkce`, authorization.searchParams.get('response_type') === 'code'
      && authorization.searchParams.get('client_id') === 'dataslate-web'
      && authorization.searchParams.get('redirect_uri') === `${APP}/auth/callback`
      && authorization.searchParams.get('code_challenge_method') === 'S256'
      && /^[A-Za-z0-9_-]{43}$/.test(challenge ?? ''));
    stage = `${role}.identity_provider_form`;
    await page.locator('#username').fill(role);
    await page.locator('#password').fill(password);
    await page.locator('#kc-login').click();
    const response = await tokenResponse;
    check(`${role}.token_exchange_response`, response?.status() === 200);
    const form = new URLSearchParams(response.request().postData() ?? '');
    const verifier = form.get('code_verifier') ?? '';
    check(`${role}.pkce_verifier_matches_challenge`, form.get('grant_type') === 'authorization_code'
      && form.get('client_id') === 'dataslate-web' && !form.has('client_secret')
      && /^[A-Za-z0-9._~-]{43,128}$/.test(verifier)
      && createHash('sha256').update(verifier).digest('base64url') === challenge);
    stage = `${role}.token_contract`;
    const token = await response.json();
    const access = jwtClaims(token.access_token);
    const identity = jwtClaims(token.id_token);
    check(`${role}.access_claims_only`, access.iss === ISSUER && identity.iss === ISSUER
      && !Object.hasOwn(identity, 'dataslate_roles') && !Object.hasOwn(identity, 'dataslate_workspaces')
      && access.dataslate_workspaces?.[role === 'outsider' ? 'team-beta' : 'team-alpha']
      && identity.aud === 'dataslate-web');
    await page.waitForURL(`${APP}/`, { timeout: 30000 });
    await page.getByRole('combobox', { name: 'Workspace', exact: true }).waitFor();
    const status = await api(page, token.access_token, '/api/v1/status');
    check(`${role}.verified_api_permissions`, status.status === 200 && status.body.authMode === 'oidc'
      && status.body.persistence === 'postgres' && status.body.permissions?.createWorkspace === (role === 'admin'));
    const idToken = await api(page, token.id_token, '/api/v1/status');
    check(`${role}.id_token_rejected_by_api`, idToken.status === 401 && idToken.body?.error?.code === 'unauthorized');
    const memberships = await api(page, token.access_token, '/api/v1/workspaces');
    check(`${role}.workspace_membership_filter`, memberships.status === 200 && memberships.body.items.length === 1
      && memberships.body.items[0].id === (role === 'outsider' ? 'team-beta' : 'team-alpha'));
    if (role !== 'admin') check(`${role}.create_workspace_hidden`, await page.getByRole('button', { name: 'Create workspace', exact: true }).count() === 0);
    await noStoredTokens(page, [token.access_token, token.id_token, token.refresh_token], role);
    return { context, page, token };
  } catch (error) {
    await context.close();
    throw error;
  }
}

async function createWorkspace(session, runId) {
  const { page, token } = session;
  stage = 'admin.workspace_creation_button';
  let postCount = 0;
  let newWorkspaceReads = 0;
  let workspaceId;
  const observe = (request) => {
    const url = new URL(request.url());
    if (url.origin === APP && url.pathname === '/api/v1/workspaces' && request.method() === 'POST') postCount++;
    if (workspaceId && url.pathname.startsWith(`/api/v1/workspaces/${workspaceId}/`)) newWorkspaceReads++;
  };
  page.on('request', observe);
  await page.getByRole('button', { name: 'Create workspace', exact: true }).first().click();
  stage = 'admin.workspace_creation_form';
  const form = page.getByRole('dialog', { name: 'Create a workspace' });
  await form.getByRole('textbox', { name: 'Workspace name' }).fill(`Core qualification ${runId}`);
  const accepted = page.waitForResponse((response) => new URL(response.url()).pathname === '/api/v1/workspaces'
    && response.request().method() === 'POST').catch(() => null);
  stage = 'admin.workspace_creation_submit';
  await form.getByRole('button', { name: 'Create workspace', exact: true }).click();
  const response = await accepted;
  await acceptedResponse('admin.workspace_created_from_ui', response, 201);
  stage = 'admin.workspace_creation_receipt';
  const receipt = page.getByRole('dialog', { name: 'Workspace created' });
  await receipt.waitFor();
  workspaceId = await receipt.getByRole('textbox', { name: 'Workspace ID' }).inputValue();
  const selector = page.getByRole('combobox', { name: 'Workspace', exact: true });
  check('admin.creation_receipt_without_membership', /^ws-[a-z0-9-]+$/.test(workspaceId)
    && (await receipt.textContent()).includes('dataslate_workspaces')
    && await selector.inputValue() === 'team-alpha'
    && await selector.locator(`option[value="${workspaceId}"]`).count() === 0);
  await receipt.getByRole('button', { name: 'Done', exact: true }).click();
  await receipt.waitFor({ state: 'detached' });
  check('admin.done_does_not_resubmit_or_select', postCount === 1 && newWorkspaceReads === 0
    && await selector.inputValue() === 'team-alpha');
  page.off('request', observe);
  const denied = await api(page, token.access_token, `/api/v1/workspaces/${workspaceId}/notebooks`);
  check('admin.created_workspace_membership_denied', denied.status === 403 && denied.body?.error?.code === 'forbidden');
  return workspaceId;
}

async function notebook(session, runId, previous) {
  const { page, token } = session;
  const name = `Core qualification ${runId}`;
  let saved;
  if (!previous) {
    stage = 'editor.notebook_save';
    await page.getByRole('button', { name: 'Create notebook', exact: true }).click();
    await page.getByRole('textbox', { name: 'Document name' }).fill(name);
    await page.locator('.cm-content').first().fill(SOURCE);
    const accepted = page.waitForResponse((response) => new URL(response.url()).pathname === '/api/v1/workspaces/team-alpha/notebooks'
      && response.request().method() === 'POST').catch(() => null);
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    const response = await accepted;
    await acceptedResponse('editor.notebook_saved_from_ui', response, 201);
    saved = await response.json();
    await page.getByText('Saved · revision 1', { exact: true }).waitFor();
  } else {
    stage = 'editor.persisted_notebook_reopen';
    const result = await api(page, token.access_token, `/api/v1/workspaces/team-alpha/notebooks/${previous.notebookId}`);
    check('editor.persisted_notebook_read', result.status === 200);
    saved = result.body;
    await page.getByRole('button', { name: `${name} Notebook · revision 1`, exact: true }).click();
    await page.getByText('Saved · revision 1', { exact: true }).waitFor();
  }
  check('editor.notebook_snapshot_matches', typeof saved.id === 'string' && saved.workspaceId === 'team-alpha'
    && saved.revision === 1 && saved.kind === 'notebook' && saved.name === name
    && saved.cells?.length === 1 && saved.cells[0].source === SOURCE
    && (!previous || saved.id === previous.notebookId));
  const read = await api(page, token.access_token, `/api/v1/workspaces/team-alpha/notebooks/${saved.id}`);
  check('editor.notebook_readback_matches', read.status === 200 && read.body.cells[0].source === SOURCE);
  check('editor.saved_source_visible', (await page.locator('.cm-content').first().innerText()).includes('dataslate-core-browser'));
  const denied = await api(page, token.access_token, '/api/v1/workspaces/team-beta/notebooks');
  check('editor.cross_workspace_denied', denied.status === 403 && denied.body?.error?.code === 'forbidden');
  return saved;
}

async function main() {
  // Keep the default separate so duplicate explicit flags still fail closed.
  const args = options(process.argv.slice(2));
  if (!args['private-dir'] || !args.output || !['seed', 'read'].includes(args.mode)
    || (args.mode === 'read') !== Boolean(args.previous)) throw new Error('invalid_arguments');
  const privateDir = resolve(args['private-dir']);
  const output = resolve(args.output);
  const privateInfo = await stat(privateDir);
  if (!privateInfo.isDirectory() || (privateInfo.mode & 0o077)) throw new Error('invalid_private_directory');
  const result = { schemaVersion: 1, mode: args.mode, status: 'running', checks, runId: randomUUID() };
  try {
    stage = 'fixture_validation';
    const credentials = await privateJSON(join(privateDir, 'credentials.json'));
    const fixture = await privateJSON(join(privateDir, 'fixture-info.json'));
    check('disposable_fixture_identity', fixture.issuer === ISSUER && fixture.applicationOrigin === APP
      && fixture.namespace === 'dataslate-core-fixture' && fixture.applicationNamespace === 'dataslate-core-test'
      && ROLES.every((role) => typeof credentials[role] === 'string' && /^[a-f0-9]{48}$/.test(credentials[role])));
    let previous;
    if (args.previous) {
      const info = await stat(args.previous);
      if (!info.isFile() || info.size > 65536) throw new Error('invalid_previous_result');
      previous = JSON.parse(await readFile(args.previous, 'utf8'));
      check('previous_seed_receipt_valid', previous.schemaVersion === 1 && previous.mode === 'seed' && previous.status === 'passed'
        && /^[a-f0-9-]{36}$/.test(previous.runId) && /^[a-z0-9-]{1,63}$/.test(previous.notebookId)
        && /^ws-[a-z0-9-]+$/.test(previous.createdWorkspaceId));
      result.runId = previous.runId;
      result.createdWorkspaceId = previous.createdWorkspaceId;
    }
    stage = 'fixture_certificate_and_browser';
    const pin = certificatePin(privateDir);
    const require = createRequire(join(ROOT, 'web/package.json'));
    const { chromium } = require('playwright');
    const executablePath = args.chrome ?? process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH
      ?? (process.platform === 'darwin' ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' : undefined);
    browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}), args: [
      '--no-proxy-server',
      '--host-resolver-rules=MAP identity.dataslate.test 127.0.0.1,MAP workspace.dataslate.test 127.0.0.1',
      `--ignore-certificate-errors-spki-list=${pin}`,
    ] });
    let saved;
    for (const role of ROLES) {
      const session = await login(role, credentials[role]);
      try {
        if (role === 'admin' && args.mode === 'seed') result.createdWorkspaceId = await createWorkspace(session, result.runId);
        if (role === 'editor') {
          saved = await notebook(session, result.runId, previous);
          result.notebookId = saved.id;
          result.workspaceId = 'team-alpha';
        }
        if (role === 'viewer') {
          const path = `/api/v1/workspaces/team-alpha/notebooks/${saved.id}`;
          const read = await api(session.page, session.token.access_token, path);
          check('viewer.notebook_read_allowed', read.status === 200 && read.body.cells[0].source === SOURCE);
          const write = await api(session.page, session.token.access_token, path, 'PUT', {
            name: saved.name, kind: saved.kind, revision: saved.revision, cells: [{ ...saved.cells[0], source: 'forbidden = True' }],
          });
          check('viewer.notebook_write_denied', write.status === 403 && write.body?.error?.code === 'forbidden');
          const after = await api(session.page, session.token.access_token, path);
          check('viewer.denied_write_preserves_notebook', after.status === 200 && after.body.revision === 1 && after.body.cells[0].source === SOURCE);
        }
        if (role === 'outsider') {
          for (const suffix of ['', `/${saved.id}`]) {
            const read = await api(session.page, session.token.access_token, `/api/v1/workspaces/team-alpha/notebooks${suffix}`);
            check(`outsider.${suffix ? 'notebook' : 'workspace'}_read_denied`, read.status === 403 && read.body?.error?.code === 'forbidden');
          }
        }
        await noStoredTokens(session.page, [session.token.access_token, session.token.id_token, session.token.refresh_token], `${role}.after_operations`);
      } finally {
        await session.context.close();
      }
    }
    result.status = 'passed';
  } catch {
    result.status = 'failed';
    result.failedCheck = stage;
    process.exitCode = 1;
  } finally {
    if (browser) await browser.close().catch(() => {});
    result.checkCount = checks.length;
    await writeFile(output, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  }
  console.log(`Core browser qualification ${result.status}; ${result.checkCount} checks recorded. No credentials, tokens, traces or screenshots were saved.`);
}

main().catch(() => {
  process.exitCode = 1;
  console.error(`Core browser qualification failed at ${stage}; no private diagnostics were printed.`);
});

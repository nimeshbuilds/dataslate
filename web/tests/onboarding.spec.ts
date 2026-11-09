import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

// UI boundary tests: the real loopback development API still authenticates the
// browser. A controlled production status response exercises onboarding logic;
// these fixtures do not claim to qualify an external OIDC provider.
async function productionStatus(page: Page, permission?: boolean) {
  await page.route('**/api/v1/status', async (route) => {
    const response = await route.fetch();
    const status = await response.json();
    delete status.permissions;
    await route.fulfill({ json: { ...status, authMode: 'oidc', persistence: 'postgres',
      ...(permission === undefined ? {} : { permissions: { createWorkspace: permission } }),
    } });
  });
}

for (const permission of [undefined, false]) {
  test(`workspace creation fails closed when verified API permission is ${permission === undefined ? 'absent' : 'false'}`, async ({ page }) => {
    await productionStatus(page, permission);
    await page.route('**/api/v1/workspaces', (route) => route.fulfill({ json: { items: [] } }));
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Create your first workspace' })).toBeVisible();
    await expect(page.getByText('Ask your administrator to grant you workspace membership.')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Create workspace', exact: true })).toHaveCount(0);
  });
}

for (const existingMembership of [false, true]) {
  test(`production workspace creation retains its receipt without granting membership${existingMembership ? ' or changing the current workspace' : ''}`, async ({ page }) => {
    await productionStatus(page, true);
    const created = { id: 'ws-awaiting-membership', name: 'Engineering onboarding', slug: 'engineering-onboarding', revision: 1,
      createdAt: '2026-11-08T12:00:00Z', updatedAt: '2026-11-08T12:00:00Z' };
    const existing = { ...created, id: 'default', name: 'Existing membership', slug: 'default' };
    let postCount = 0;
    const unauthorizedRequests: string[] = [];
    page.on('request', (request) => {
      if (request.url().includes(`/api/v1/workspaces/${created.id}/`)) unauthorizedRequests.push(request.url());
    });
    await page.route('**/api/v1/workspaces', async (route) => {
      if (route.request().method() === 'POST') {
        postCount++;
        expect(route.request().postDataJSON()).toEqual({ name: created.name, slug: created.slug });
        await route.fulfill({ status: 201, json: created });
      } else {
        await route.fulfill({ json: { items: existingMembership ? [existing] : [] } });
      }
    });
    await page.goto('/');
    await expect(page.getByRole('button', { name: 'Create workspace', exact: true }).first()).toBeVisible();
    const selector = page.getByRole('combobox', { name: 'Workspace', exact: true });
    await expect(selector).toHaveValue(existingMembership ? 'default' : '');
    await page.getByRole('button', { name: 'Create workspace', exact: true }).first().click();
    const form = page.getByRole('dialog', { name: 'Create a workspace' });
    await form.getByRole('textbox', { name: 'Workspace name' }).fill(created.name);
    await form.getByRole('button', { name: 'Create workspace', exact: true }).click();

    const receipt = page.getByRole('dialog', { name: 'Workspace created' });
    await expect(receipt).toBeVisible();
    await expect(receipt.getByRole('textbox', { name: 'Workspace ID' })).toHaveValue(created.id);
    await expect(receipt).toContainText('Your identity administrator must grant membership');
    await expect(receipt).toContainText('dataslate_workspaces');
    await expect(receipt).toContainText('API access token');
    await expect(receipt).toContainText('Sign out and sign in again');
    await expect(selector).toHaveValue(existingMembership ? 'default' : '');
    await expect(selector.locator(`option[value="${created.id}"]`)).toHaveCount(0);
    await expect(receipt.getByRole('button', { name: 'Create workspace', exact: true })).toHaveCount(0);
    expect(postCount).toBe(1);
    expect(unauthorizedRequests).toEqual([]);

    await receipt.getByRole('button', { name: 'Done', exact: true }).click();
    await expect(receipt).toHaveCount(0);
    await expect(selector).toHaveValue(existingMembership ? 'default' : '');
    await expect(selector.locator(`option[value="${created.id}"]`)).toHaveCount(0);
    if (!existingMembership) await expect(page.getByRole('heading', { name: 'Create your first workspace' })).toBeVisible();
    else await expect(page.getByRole('button', { name: 'Create notebook', exact: true })).toBeVisible();
    expect(postCount).toBe(1);
    expect(unauthorizedRequests).toEqual([]);
  });
}

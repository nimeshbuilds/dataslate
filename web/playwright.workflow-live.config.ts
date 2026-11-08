import { defineConfig, devices } from '@playwright/test';

if (!process.env.DATASLATE_WORKFLOW_BINDING_ID || !process.env.DATASLATE_WORKFLOW_EVIDENCE_FILE) throw new Error('Set DATASLATE_WORKFLOW_BINDING_ID and DATASLATE_WORKFLOW_EVIDENCE_FILE for an explicitly started local PostgreSQL/Airflow qualification harness on API port 8080.');
export default defineConfig({
  testDir: './qualification', testMatch: 'workflow-live.spec.ts', fullyParallel: false, workers: 1, timeout: 90000, retries: 0, reporter: 'list', outputDir: 'test-results-workflow-live',
  use: { baseURL: 'http://127.0.0.1:5173', trace: 'off', screenshot: 'off', launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH } : {} },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  // API, PostgreSQL, Airflow and its private credentials are independently owned by the harness.
  webServer: { command: 'npm run dev', url: 'http://127.0.0.1:5173', reuseExistingServer: false, timeout: 30000 },
});

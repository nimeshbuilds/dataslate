import { defineConfig, devices } from '@playwright/test';

if (!['prepare', 'resume'].includes(process.env.DATASLATE_MEMORY_PHASE ?? '') || !process.env.DATASLATE_MEMORY_STATE_FILE || !process.env.DATASLATE_MEMORY_EVIDENCE_DIR) throw new Error('Set DATASLATE_MEMORY_PHASE=prepare|resume, DATASLATE_MEMORY_STATE_FILE and DATASLATE_MEMORY_EVIDENCE_DIR for the explicitly started PostgreSQL/HTTPS qualification harness.');
export default defineConfig({
  testDir: './qualification', testMatch: 'memory-live.spec.ts', fullyParallel: false, workers: 1, timeout: 60000, retries: 0, reporter: 'list', outputDir: 'test-results-memory-live',
  use: { baseURL: 'http://127.0.0.1:5173', trace: 'off', screenshot: 'off', launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH } : {} },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  // API, PostgreSQL and HTTPS provider fixture are independently owned by the harness.
  webServer: { command: 'npm run dev', url: 'http://127.0.0.1:5173', reuseExistingServer: false, timeout: 30000 },
});

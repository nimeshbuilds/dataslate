import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: false,
  timeout: 30000,
  retries: process.env.CI ? 1 : 0,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:5173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH } : {},
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: [
    { command: 'cd ../api && go run ./cmd/dataslate-api', url: 'http://127.0.0.1:8080/healthz', env: { DATASLATE_AUTH_MODE: 'development', DATASLATE_ADDR: '127.0.0.1:8080', DATASLATE_DATABASE_URL: '', DATASLATE_EXECUTION_BINDINGS_FILE: '' }, reuseExistingServer: false, timeout: 120000 },
    { command: 'npm run dev', url: 'http://127.0.0.1:5173', reuseExistingServer: false, timeout: 30000 },
  ],
});

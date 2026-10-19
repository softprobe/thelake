import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: /.*\.spec\.ts$/,
  // The dedicated Explorer E2E owns and starts its own thelake process via
  // scripts/test-explorer-ui.sh. This Grafana suite has a separate shared
  // stack on port 8090 and intentionally does not provide that backend.
  testIgnore: ['e2e-explorer-ui.spec.ts'],
  timeout: 60000,
  expect: {
    timeout: 15000,
  },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  outputDir: process.env.GRAFANA_PLAYWRIGHT_OUTPUT_DIR || 'test-results',
  reporter: [
    ['list'],
    ['html', { outputFolder: process.env.GRAFANA_PLAYWRIGHT_REPORT_DIR || 'playwright-report', open: 'never' }],
  ],
  use: {
    baseURL: process.env.GRAFANA_URL || 'http://127.0.0.1:3000',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    headless: true,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});

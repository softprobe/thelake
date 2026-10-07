import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: ['e2e_explorer_ui.spec.ts'],
  timeout: 30000,
  expect: { timeout: 10000 },
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    headless: true,
    ...devices['Desktop Chrome'],
  },
  webServer: {
    command: 'npm --prefix ../../../../packages/thelake-explorer run dev:spa',
    url: 'http://127.0.0.1:4173/explorer/',
    reuseExistingServer: !process.env.CI,
    timeout: 30000,
  },
});

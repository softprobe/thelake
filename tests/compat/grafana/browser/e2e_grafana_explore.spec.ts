import { test, expect, Page } from '@playwright/test';
import { QUERY_FEATURE_CATALOG } from './query_features';

async function loginToGrafana(page: Page) {
  await page.goto('/login');
  await page.fill('input[name="user"]', 'admin');
  await page.fill('input[name="password"]', 'admin');
  await page.click('button[type="submit"]');

  try {
    const skipBtn = page.locator('text=Skip');
    await skipBtn.waitFor({ timeout: 3000 });
    await skipBtn.click();
  } catch (e) {}

  await page.waitForURL('**/', { timeout: 10000 });
}

test.describe('Grafana Explore Interactive Query Automation (Browser)', () => {
  test.describe.configure({ mode: 'serial' });

  let page: Page;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext();
    page = await context.newPage();
    await loginToGrafana(page);
  });

  test.afterAll(async () => {
    await page?.context()?.close();
  });

  for (const item of QUERY_FEATURE_CATALOG) {
    test(`Explore Query [${item.id} - ${item.category}]: ${item.expr}`, async () => {
      const paneQuery = encodeURIComponent(
        JSON.stringify({
          datasource: 'softprobe-loki-a',
          queries: [{ refId: 'A', expr: item.expr, range: item.isRange, queryType: 'range' }],
          range: { from: 'now-1h', to: 'now' },
        }),
      );

      await page.goto(`/explore?schemaVersion=1&panes={"left":${paneQuery}}`);
      await page.waitForTimeout(2000);

      const title = await page.title();
      expect(title).toContain('Explore');

      const runBtn = page.locator('[data-testid="data-testid RefreshPicker run button"]');
      if (await runBtn.isVisible()) {
        await runBtn.click();
      }
      await page.waitForTimeout(3000);

      const logElements = await page.$$eval(
        '[data-testid*="log"], .logs-row, [class*="logs-row"], pre, [data-testid*="panel"], [data-testid*="Query"]',
        (elements) => elements.length,
      );
      expect(logElements).toBeGreaterThan(0);

      const errorLocator = page.locator('[data-testid="data-testid Alert error"]');
      const isErrorVisible = await errorLocator.isVisible().catch(() => false);
      expect(isErrorVisible).toBe(false);
    });
  }
});

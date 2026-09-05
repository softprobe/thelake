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
    const isLoki = item.category === 'loki';
    const datasource = isLoki ? 'softprobe-loki-a' : 'softprobe-prom-a';

    test(`Explore Query [${item.id} - ${item.category}]: ${item.expr}`, async () => {
      const paneQuery = encodeURIComponent(JSON.stringify({
        datasource,
        queries: [{ refId: 'A', expr: item.expr, range: item.isRange, queryType: isLoki ? 'range' : undefined }],
        range: { from: 'now-1h', to: 'now' },
      }));

      await page.goto(`/explore?schemaVersion=1&panes={"left":${paneQuery}}`);
      await page.waitForTimeout(2000);

      // Verify page is on Explore
      const title = await page.title();
      expect(title).toContain('Explore');

      if (!isLoki) {
        // For PromQL, verify that graph or legend elements are rendered
        const legendItems = await page.$$eval(
          '.legend-item, [class*="LegendItem"], [data-testid*="legend"], canvas',
          elements => elements.length
        );
        expect(legendItems).toBeGreaterThan(0);
      } else {
        // Click run query if needed
        const runBtn = page.locator('[data-testid="data-testid RefreshPicker run button"]');
        if (await runBtn.isVisible()) {
          await runBtn.click();
        }
        await page.waitForTimeout(3000);

        // For Loki, verify log stream rows, message container, or query status is rendered
        const logElements = await page.$$eval(
          '[data-testid*="log"], .logs-row, [class*="logs-row"], pre, [data-testid*="panel"], [data-testid*="Query"]',
          elements => elements.length
        );
        expect(logElements).toBeGreaterThan(0);
      }

      // Assert that NO visible error alert is shown
      const errorLocator = page.locator('[data-testid="data-testid Alert error"]');
      const isErrorVisible = await errorLocator.isVisible().catch(() => false);
      expect(isErrorVisible).toBe(false);
    });
  }
});

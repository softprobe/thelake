import { test, expect, Page } from '@playwright/test';

const EXPLORE_CHECKLIST_QUERIES = [
  {
    category: 'Selector',
    expr: 'k6_http_reqs{job="load-generator"}',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Rate Family',
    expr: 'rate(k6_http_reqs[5m])',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Gauge Delta',
    expr: 'delta(k6_vus[5m])',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Vector Aggregation (sum by)',
    expr: 'sum by (job) (k6_http_reqs)',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Vector Aggregation (avg without)',
    expr: 'avg without (method) (k6_http_reqs)',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Over-Time Aggregation',
    expr: 'avg_over_time(k6_vus[5m])',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Math Function',
    expr: 'abs(delta(k6_vus[5m]))',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Binary Arithmetic',
    expr: 'k6_vus * 2',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Binary Ratio (vector / vector)',
    expr: 'sum(k6_http_reqs) / sum(k6_vus)',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Comparison with bool modifier',
    expr: 'k6_vus > bool 1',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Set Operator (and)',
    expr: 'sum(k6_http_reqs) and sum(k6_vus)',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Time Offset Modifier',
    expr: 'k6_http_reqs offset 1m',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Classic Histogram Bucket Grouping',
    expr: 'sum by (le) (http_server_request_duration_bucket)',
    datasource: 'softprobe-prom-a',
    isLoki: false,
  },
  {
    category: 'Loki Log Stream Query',
    expr: '{service_name=~".+"}',
    datasource: 'softprobe-loki-a',
    isLoki: true,
  },
];

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

  for (const item of EXPLORE_CHECKLIST_QUERIES) {
    test(`Explore Query [${item.category}]: ${item.expr}`, async () => {
      const paneQuery = encodeURIComponent(JSON.stringify({
        datasource: item.datasource,
        queries: [{ refId: 'A', expr: item.expr, range: true }],
        range: { from: 'now-1h', to: 'now' },
      }));

      await page.goto(`/explore?schemaVersion=1&panes={"left":${paneQuery}}`);
      await page.waitForTimeout(2500);

      // Verify page is on Explore
      const title = await page.title();
      expect(title).toContain('Explore');

      if (!item.isLoki) {
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
        await page.waitForTimeout(5000);

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

import { test, expect, Page } from '@playwright/test';

const DASHBOARDS_UNDER_TEST = [
  // PromQL Capability Dashboards
  { uid: 'softprobe-prom-aggregations', title: 'Softprobe · Aggregations' },
  { uid: 'softprobe-prom-operators', title: 'Softprobe · Arithmetic, compare, set ops' },
  { uid: 'softprobe-prom-histograms', title: 'Softprobe · Classic histogram series' },
  { uid: 'softprobe-prom-over-time', title: 'Softprobe · over_time, math, offset' },
  { uid: 'softprobe-prom-overview', title: 'Softprobe · Overview (Astronomy Shop)' },
  { uid: 'softprobe-prom-rate', title: 'Softprobe · rate / irate / increase / delta' },
  { uid: 'softprobe-prom-selectors', title: 'Softprobe · Selectors & matchers' },
  { uid: 'softprobe-prom-smoke', title: 'Softprobe Prometheus smoke' },

  // Astronomy Shop Service Dashboards
  { uid: 'astronomy-shop-overview', title: 'Astronomy Shop · GOLD overview' },
  { uid: 'astronomy-shop-loadgen', title: 'Astronomy Shop · Load generator (k6)' },
  { uid: 'astronomy-shop-ad', title: 'Astronomy Shop · Ad (Java)' },
  { uid: 'astronomy-shop-cart', title: 'Astronomy Shop · Cart (.NET)' },
  { uid: 'astronomy-shop-checkout', title: 'Astronomy Shop · Checkout (Go)' },
  { uid: 'astronomy-shop-frontend', title: 'Astronomy Shop · Frontend (Node.js)' },
  { uid: 'astronomy-shop-payment', title: 'Astronomy Shop · Payment (Node.js)' },
  { uid: 'astronomy-shop-shipping', title: 'Astronomy Shop · Shipping' },
  { uid: 'astronomy-shop-currency-quote', title: 'Astronomy Shop · Currency & Quote' },
  { uid: 'astronomy-shop-infra', title: 'Astronomy Shop · Infrastructure' },
  { uid: 'astronomy-shop-product-catalog', title: 'Astronomy Shop · Product Catalog' },
  { uid: 'astronomy-shop-recommendation', title: 'Astronomy Shop · Recommendation (Python)' },

  // Smoke & Cross-Signal Dashboards
  { uid: 'softprobe-loki-smoke', title: 'Softprobe Loki smoke' },
  { uid: 'softprobe-cross-signal', title: 'Softprobe cross-signal smoke' },
];

async function loginToGrafana(page: Page) {
  await page.goto('/login');
  await page.fill('input[name="user"]', 'admin');
  await page.fill('input[name="password"]', 'admin');
  await page.click('button[type="submit"]');

  // Handle optional "Update your password" screen
  try {
    const skipBtn = page.locator('text=Skip');
    await skipBtn.waitFor({ timeout: 3000 });
    await skipBtn.click();
  } catch (e) {
    // Already skipped or disabled
  }

  await page.waitForURL('**/', { timeout: 10000 });
}

test.describe('Real Grafana Dashboard Rendering & Settings (Browser Automation)', () => {
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

  for (const dash of DASHBOARDS_UNDER_TEST) {
    test(`Dashboard loads and renders: ${dash.title} [${dash.uid}]`, async () => {
      // Capture any unhandled console errors
      const pageErrors: string[] = [];
      page.on('pageerror', err => pageErrors.push(err.message));

      await page.goto(`/d/${dash.uid}/`);
      
      // Wait for dashboard panels to mount
      await page.locator('[data-testid*="panel"], [data-testid*="Panel"], h2, h3, canvas, .panel-title').first().waitFor({ timeout: 15000 });
      await page.waitForTimeout(1000);

      // Verify page title contains dashboard title
      const title = await page.title();
      expect(title).toContain(dash.title);

      // Verify panels or canvas charts rendered
      const canvasCount = await page.$$eval('canvas', els => els.length);
      const panelCount = await page.$$eval('[data-testid*="panel"], [data-testid*="Panel"], h2, h3, .panel-title', els => els.length);
      expect(canvasCount + panelCount).toBeGreaterThan(0);

      // Verify no panel error alerts are visible on the dashboard
      const visibleErrors = await page.$$eval(
        '[data-testid="data-testid Alert error"]',
        elements => elements.filter(el => {
          const style = window.getComputedStyle(el);
          return style.display !== 'none' && style.opacity !== '0' && style.visibility !== 'hidden';
        }).map(e => (e as HTMLElement).innerText)
      );

      expect(visibleErrors).toEqual([]);
      expect(pageErrors.filter(e => !e.includes('ResizeObserver'))).toEqual([]);
    });
  }
});

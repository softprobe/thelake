import { test, expect, Page } from '@playwright/test';

const DASHBOARDS_UNDER_TEST = [
  { uid: 'softprobe-loki-smoke', title: 'Softprobe Loki smoke' },
  { uid: 'softprobe-tempo-smoke', title: 'Softprobe Tempo smoke' },
  { uid: 'softprobe-cross-signal', title: 'Softprobe cross-signal smoke' },
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
      const pageErrors: string[] = [];
      page.on('pageerror', (err) => pageErrors.push(err.message));

      await page.goto(`/d/${dash.uid}/`);

      await page
        .locator('[data-testid*="panel"], [data-testid*="Panel"], h2, h3, canvas, .panel-title')
        .first()
        .waitFor({ timeout: 15000 });
      await page.waitForTimeout(1000);

      const title = await page.title();
      expect(title).toContain(dash.title);

      const canvasCount = await page.$$eval('canvas', (els) => els.length);
      const panelCount = await page.$$eval(
        '[data-testid*="panel"], [data-testid*="Panel"], h2, h3, .panel-title',
        (els) => els.length,
      );
      expect(canvasCount + panelCount).toBeGreaterThan(0);

      const visibleErrors = await page.$$eval(
        '[data-testid="data-testid Alert error"]',
        (elements) =>
          elements
            .filter((el) => {
              const style = window.getComputedStyle(el);
              return (
                style.display !== 'none' &&
                style.opacity !== '0' &&
                style.visibility !== 'hidden'
              );
            })
            .map((e) => (e as HTMLElement).innerText),
      );

      expect(visibleErrors).toEqual([]);
      expect(pageErrors.filter((e) => !e.includes('ResizeObserver'))).toEqual([]);
    });
  }
});

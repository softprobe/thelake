import { expect, test } from '@playwright/test';

const sessions = {
  items: [
    {
      session_id: 'session-e2e-001',
      start_time: '2026-10-06T12:00:00.000Z',
      end_time: '2026-10-06T12:00:02.000Z',
      trace_count: 1,
      observation_count: 2,
      error_count: 1,
      total_tokens: 24,
      total_cost: 0.0003,
      user_ids: ['user-e2e-001'],
      models: ['gpt-test'],
    },
    {
      session_id: 'session-e2e-002',
      start_time: '2026-10-06T11:00:00.000Z',
      trace_count: 1,
      observation_count: 1,
      error_count: 0,
      user_ids: ['user-e2e-002'],
      models: ['claude-test'],
    },
  ],
  next_cursor: null,
};

test('session list shows summary, filters sessions, and opens trace details', async ({ page }) => {
  await page.route('**/v1/llm/sessions/search', async (route) => {
    const body = route.request().postDataJSON();
    expect(body).toMatchObject({ limit: 50, roots_only: true, order: 'desc' });
    await route.fulfill({ json: sessions });
  });

  await page.route('**/v1/llm/sessions/session-e2e-001?*', async (route) => {
    await route.fulfill({
      json: {
        session_id: 'session-e2e-001',
        traces: [
          {
            trace_id: 'trace-e2e-001',
            name: 'agent run',
            start_time: '2026-10-06T12:00:00.000Z',
            end_time: '2026-10-06T12:00:02.000Z',
            observation_count: 2,
            error_count: 1,
            total_tokens: 24,
            total_cost: 0.0003,
          },
        ],
        next_cursor: null,
      },
    });
  });

  await page.route('**/v1/llm/traces/trace-e2e-001?*', async (route) => {
    await route.fulfill({
      json: {
        observations: [
          {
            trace_id: 'trace-e2e-001',
            span_id: 'span-e2e-001',
            session_id: 'session-e2e-001',
            name: 'Agent execution',
            observation_type: 'agent',
            start_time: '2026-10-06T12:00:00.000Z',
            end_time: '2026-10-06T12:00:02.000Z',
            status_code: 'OK',
            model_name: 'gpt-test',
            input_tokens: 10,
            output_tokens: 14,
            total_tokens: 24,
            input: 'Calculate 2 + 2',
            output: '4',
            attributes: { 'agent.name': 'calculator' },
            events: [],
            scores: [],
          },
          {
            trace_id: 'trace-e2e-001',
            span_id: 'span-e2e-002',
            parent_span_id: 'span-e2e-001',
            session_id: 'session-e2e-001',
            name: 'Tool execution',
            observation_type: 'tool',
            start_time: '2026-10-06T12:00:01.000Z',
            end_time: '2026-10-06T12:00:02.000Z',
            status_code: 'ERROR',
            input: { expression: '2 + 2' },
            output: { result: 4 },
            attributes: {},
            events: [],
            scores: [],
          },
        ],
        next_cursor: null,
      },
    });
  });

  await page.goto('/explorer/');

  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
  const firstSession = page.getByRole('button', { name: /session-e2e-001/ });
  await expect(firstSession).toContainText('1 traces · 2 spans · 1 errors');
  await expect(firstSession).toContainText('gpt-test');

  const filter = page.getByRole('textbox', { name: 'Filter sessions' });
  await filter.fill('claude-test');
  await expect(page.getByRole('button', { name: /session-e2e-002/ })).toBeVisible();
  await expect(firstSession).toHaveCount(0);

  await filter.fill('');
  await firstSession.click();
  await expect(page.getByRole('heading', { name: 'Session session-e2e-001' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Trace trace-e2e-001' })).toBeVisible();
  await expect(page.getByRole('article').getByText('Agent execution')).toBeVisible();
  await expect(page.getByText('Calculate 2 + 2')).toBeVisible();
  await expect(page.getByText('4', { exact: true })).toBeVisible();
});

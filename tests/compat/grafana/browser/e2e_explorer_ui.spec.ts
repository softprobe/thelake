import { expect, test } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { readFileSync } from 'node:fs';

const backend = process.env.THELAKE_E2E_URL ?? 'http://127.0.0.1:18090';
const suffix = `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;

function otlpValue(value: string | number) {
  return typeof value === 'number'
    ? { intValue: String(value) }
    : { stringValue: value };
}

function span({
  traceId,
  spanId,
  parentSpanId,
  sessionId,
  name,
  type,
  start,
  attributes = {},
  events = [],
}: {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  sessionId: string;
  name: string;
  type: string;
  start: Date;
  attributes?: Record<string, string | number>;
  events?: Array<{ name: string; attributes?: Record<string, string> }>;
}) {
  const startNanos = BigInt(start.getTime()) * 1_000_000n;
  return {
    traceId,
    spanId,
    parentSpanId: parentSpanId ?? '',
    name,
    kind: 2,
    startTimeUnixNano: String(startNanos),
    endTimeUnixNano: String(startNanos + 25_000_000n),
    attributes: Object.entries({ 'sp.session.id': sessionId, 'sp.observation.type': type, ...attributes })
      .map(([key, value]) => ({ key, value: otlpValue(value) })),
    events: events.map((event, index) => ({
      timeUnixNano: String(startNanos + BigInt(index + 1) * 1_000_000n),
      name: event.name,
      attributes: Object.entries(event.attributes ?? {}).map(([key, value]) => ({ key, value: { stringValue: value } })),
    })),
    status: { code: 1 },
  };
}

async function openDetails(page: import('@playwright/test').Page, label: string) {
  const summary = page.getByText(label, { exact: true });
  const details = summary.locator('xpath=..');
  if (!await details.evaluate((element) => (element as HTMLDetailsElement).open)) await summary.click();
}

function runRefundAgent(agentName: string): string {
  const composeFile = resolve(process.cwd(), '../../../../examples/quickstart/compose.yaml');
  const project = process.env.THELAKE_E2E_COMPOSE_PROJECT;
  const lakePort = process.env.THELAKE_E2E_LAKE_PORT;
  expect(project, 'online E2E compose project must be configured').toBeTruthy();
  expect(lakePort, 'online E2E lake port must be configured').toBeTruthy();
  const agentApiHost = process.platform === 'linux' ? '127.0.0.1' : 'host.docker.internal';
  const agentOutput = execFileSync('docker', [
    'compose', '--project-name', project!, '--file', composeFile,
    'run', '--rm', 'refund-agent', '--agent-name', agentName,
    '--api-url', `http://${agentApiHost}:${lakePort}`,
  ], { encoding: 'utf8', timeout: 150_000, env: process.env });
  const sessionId = agentOutput.match(/session_id=([A-Za-z0-9._-]+)/)?.[1];
  expect(sessionId, `real demo agent did not report a session ID:\n${agentOutput}`).toBeTruthy();
  return sessionId!;
}

test('live thelake ingest, reducer, session filters, and trace detail render in Explorer', async ({ page, request }) => {
  test.setTimeout(120_000);
  const sessions = {
    alpha: `explorer-e2e-alpha-${suffix}`,
    beta: `explorer-e2e-beta-${suffix}`,
    old: `explorer-e2e-old-${suffix}`,
  };
  const now = new Date();
  const makeSession = (sessionId: string, agent: string, ageHours: number, tokenCount: number) => {
    const start = new Date(now.getTime() - ageHours * 60 * 60 * 1000);
    const traceId = randomUUID().replace(/-/g, '');
    const agentSpanId = Math.floor(Math.random() * 0xffffffff).toString(16).padStart(16, '0');
    const toolSpanId = (BigInt(`0x${agentSpanId}`) + 1n).toString(16).padStart(16, '0');
    const reasoningSpanId = (BigInt(`0x${agentSpanId}`) + 2n).toString(16).padStart(16, '0');
    return [
      span({ traceId, spanId: agentSpanId, sessionId, name: agent, type: 'agent', start }),
      span({
        traceId, spanId: toolSpanId, parentSpanId: agentSpanId, sessionId,
        name: 'lookup_customer', type: 'tool', start: new Date(start.getTime() + 5),
        attributes: { 'sp.input': JSON.stringify({ customer_id: 'cust-e2e-42' }), 'sp.output': JSON.stringify({ plan: 'pro' }) },
        events: [{ name: 'tool.started', attributes: { tool: 'lookup_customer' } }, { name: 'tool.completed', attributes: { status: 'ok' } }],
      }),
      span({
        traceId, spanId: reasoningSpanId, parentSpanId: agentSpanId, sessionId,
        name: 'reasoning', type: 'generation', start: new Date(start.getTime() + 10),
        attributes: {
          'gen_ai.request.model': 'gpt-e2e',
          'gen_ai.usage.input_tokens': tokenCount,
          'gen_ai.usage.output_tokens': 7,
          'gen_ai.usage.total_tokens': tokenCount + 7,
          'sp.input': 'Decide which plan applies to this customer',
          'sp.output': 'The customer is on the pro plan.',
        },
        events: [{ name: 'reasoning.step', attributes: { content: 'Matched the active subscription.' } }],
      }),
    ];
  };

  const ingest = await request.post(`${backend}/v1/traces`, {
    data: {
      resourceSpans: [{
        resource: { attributes: [{ key: 'service.name', value: { stringValue: 'explorer-e2e' } }] },
        scopeSpans: [{ scope: { name: 'softprobe.explorer.e2e' }, spans: [
          ...makeSession(sessions.alpha, 'agent-alpha-e2e', 0.1, 11),
          ...makeSession(sessions.beta, 'agent-beta-e2e', 0.2, 17),
          ...makeSession(sessions.old, 'agent-alpha-e2e', 48, 23),
        ] }],
      }],
    },
  });
  expect(ingest.ok(), `OTLP ingest failed: ${ingest.status()} ${await ingest.text()}`).toBeTruthy();
  expect((await ingest.json()).ingested_count).toBe(9);

  // Poll the real list endpoint until the asynchronous DuckLake flush and
  // Postgres session-summary reducer have produced all three summaries.
  const query = async (days: number, agentName?: string) => {
    const to = new Date();
    const result = await request.post(`${backend}/v1/sessions/search`, {
      data: {
        from: new Date(to.getTime() - days * 86_400_000).toISOString(),
        to: to.toISOString(), order_by: 'start_time', order: 'desc', limit: 50,
        roots_only: true, ...(agentName ? { agent_name: agentName } : {}),
      },
    });
    expect(result.ok(), `session search failed: ${result.status()} ${await result.text()}`).toBeTruthy();
    return result.json();
  };
  const expectedSessionIds = Object.values(sessions);
  await expect.poll(async () => {
    const result = await query(7);
    return result.items?.map((item: { session_id: string }) => item.session_id).filter((id: string) => expectedSessionIds.includes(id)) ?? [];
  }, { timeout: 60_000, intervals: [250, 500, 1000, 2000] }).toEqual(expect.arrayContaining(expectedSessionIds));

  const generatedSummaries = await query(7);
  const alphaSummary = generatedSummaries.items.find((item: { session_id: string }) => item.session_id === sessions.alpha);
  expect(alphaSummary).toMatchObject({ session_id: sessions.alpha, agent_name: 'agent-alpha-e2e', span_count: 3 });
  expect(alphaSummary.models).toContain('gpt-e2e');
  expect(alphaSummary.total_tokens).toBe(18);

  await page.goto('/explorer/');
  await page.getByRole('button', { name: 'Sessions', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();

  const range = page.getByRole('combobox', { name: 'Session time range' });
  await range.selectOption('1');
  await expect(page.getByRole('button', { name: new RegExp(sessions.alpha) })).toBeVisible();
  await expect(page.getByRole('button', { name: new RegExp(sessions.beta) })).toBeVisible();
  await expect(page.getByRole('button', { name: new RegExp(sessions.old) })).toHaveCount(0);
  await range.selectOption('7');
  await expect(page.getByRole('button', { name: new RegExp(sessions.old) })).toBeVisible();

  const sessionFilter = page.getByRole('textbox', { name: 'Filter sessions' });
  await sessionFilter.fill(sessions.alpha);
  await expect(page.getByRole('button', { name: new RegExp(sessions.alpha) })).toBeVisible();
  await expect(page.getByRole('button', { name: new RegExp(sessions.beta) })).toHaveCount(0);
  await sessionFilter.fill('');

  const agentFilter = page.getByRole('textbox', { name: 'Filter by agent' });
  await agentFilter.fill('agent-beta-e2e');
  await expect(page.getByRole('button', { name: new RegExp(sessions.beta) })).toBeVisible();
  await expect(page.getByRole('button', { name: new RegExp(sessions.alpha) })).toHaveCount(0);
  await expect(page.getByRole('button', { name: new RegExp(sessions.old) })).toHaveCount(0);

  await agentFilter.fill('');
  const alphaButton = page.getByRole('button', { name: new RegExp(sessions.alpha) });
  await expect(alphaButton).toContainText('3 spans · 0 errors');
  await expect(alphaButton).toContainText('gpt-e2e');
  await alphaButton.click();

  await expect(page.getByRole('heading', { name: `Session ${sessions.alpha}` })).toBeVisible();
  await expect(page.getByRole('heading', { name: /Trace [a-f0-9]+/ })).toBeVisible();
  await expect(page.getByRole('button', { name: /lookup_customer/ })).toBeVisible();
  await expect(page.getByRole('button', { name: /reasoning/ })).toBeVisible();
  await page.getByRole('button', { name: /lookup_customer/ }).click();
  await expect(page.locator('.tle-payload').getByText('{"customer_id":"cust-e2e-42"}', { exact: true })).toBeVisible();
  await openDetails(page, 'Attributes and events');
  await expect(page.getByText('tool.started')).toBeVisible();
  await page.getByRole('button', { name: /reasoning/ }).click();
  await openDetails(page, 'Attributes and events');
  await expect(page.getByText('Matched the active subscription.')).toBeVisible();
  await expect(page.locator('.tle-payload').getByText('The customer is on the pro plan.', { exact: true })).toBeVisible();
});

test('Markdown policy rubric detects a live trace', async ({ page, request }) => {
  test.skip(process.env.THELAKE_EXPLORER_E2E_ONLINE !== '1', 'requires the real online-evaluation E2E stack');
  test.setTimeout(240_000);

  const agentName = `policy-refund-agent-${suffix}`;
  const evaluatorId = `policy-dogfood-${suffix}`;
  const policyContent = readFileSync(
    resolve(process.cwd(), '../../../../examples/policy-dogfood/refund-eligibility/POLICY.md'),
    'utf8',
  );
  const criteria = `${policyContent}\n\nEvaluate the session against this confirmed rule. Cite the source as examples/policy-dogfood/refund-eligibility/POLICY.md.`;

  const evaluatorResponse = await request.post(`${backend}/v1/evaluators`, {
    data: {
      evaluator_id: evaluatorId,
      version: 1,
      target_agent_name: agentName,
      name: 'Refund policy dogfood',
      criteria,
    },
  });
  expect(evaluatorResponse.status(), await evaluatorResponse.text()).toBe(201);
  const savedEvaluator = await evaluatorResponse.json() as { criteria: string };
  expect(savedEvaluator.criteria).toContain('examples/policy-dogfood/refund-eligibility/POLICY.md');

  const activation = await request.post(`${backend}/v1/evaluators/${evaluatorId}/versions/1/activate`);
  expect(activation.ok(), await activation.text()).toBeTruthy();

  const sessionId = runRefundAgent(agentName);
  const sessionUrl = `${backend}/v1/sessions/${encodeURIComponent(sessionId)}?limit=200`;
  type EvaluationScore = { score_id: string; source?: string; string_value?: string; comment?: string; config_id?: string; span_id?: string; metadata?: Record<string, string> };
  let detail: { spans?: Array<{ span_id: string }>; scores?: EvaluationScore[] } | undefined;
  await expect.poll(async () => {
    const response = await request.get(sessionUrl);
    if (!response.ok()) return 'session unavailable';
    detail = await response.json();
    const score = detail.scores?.find((item) => item.config_id === `evaluator:${evaluatorId}:v1`);
    return score ? `${score.string_value}: ${score.comment ?? ''}` : `waiting for score; spans=${detail.spans?.length ?? 0}`;
  }, { timeout: 120_000, intervals: [1_000, 2_000, 4_000] }).toContain('fail:');

  const failure = detail?.scores?.find((item) => item.config_id === `evaluator:${evaluatorId}:v1` && item.string_value === 'fail');
  expect(failure?.source).toBe('evaluator');
  expect(failure?.span_id, 'failure must reference evidence in the stored trace').toBeTruthy();
  expect(detail?.spans?.some((span) => span.span_id === failure?.span_id)).toBe(true);

  await page.goto(`${backend}/explorer/`);
  await expect(page.getByRole('region', { name: 'TheLake chat' })).toBeVisible();
  await expect(page.getByText('Refund policy dogfood', { exact: true })).toBeVisible();
});

test('embedded web chat to real Gemini agent, online evaluator, persisted issue, and trace details', async ({ page, request }) => {
  test.skip(process.env.THELAKE_EXPLORER_E2E_ONLINE !== '1', 'requires the real online-evaluation E2E stack');
  test.setTimeout(240_000);
  const agentName = `refund-agent-e2e-${suffix}`;

  await page.goto(`${backend}/explorer/`);
  await expect(page.getByRole('region', { name: 'TheLake chat' })).toBeVisible();
  await page.getByRole('button', { name: 'Create a behavior check' }).click();
  const composer = page.getByRole('textbox', { name: 'Message theLake' });
  await composer.fill('Before issuing a refund, verify the ticket is eligible and explain the result.');
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.getByText(/Which agent should I watch\?/)).toBeVisible();
  await composer.fill(agentName);
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.getByRole('heading', { name: 'Review behavior check' })).toBeVisible();
  await page.getByRole('checkbox', { name: 'I understand and want to activate this check.' }).check();
  await page.getByRole('button', { name: 'Activate check' }).click();
  await expect(page.getByText(/Behavior check is active for/)).toBeVisible({ timeout: 30_000 });

  const sessionId = runRefundAgent(agentName);

  const sessionUrl = `${backend}/v1/sessions/${encodeURIComponent(sessionId!)}?limit=200`;
  type EvaluatorScore = { score_id: string; name: string; source?: string; string_value?: string; comment?: string; config_id?: string; span_id?: string };
  let detail: { spans?: Array<{ span_id: string; name: string }>; scores?: EvaluatorScore[] } | undefined;
  await expect.poll(async () => {
    const response = await request.get(sessionUrl);
    if (!response.ok()) return undefined;
    detail = await response.json();
    const scores = detail.scores?.filter((score) => score.source === 'evaluator' && score.config_id?.startsWith('evaluator:')) ?? [];
    return scores.length ? scores.map((score) => `${score.string_value}: ${score.comment ?? ''}`).join('\n') : `no evaluator scores; spans=${detail.spans?.length ?? 0}`;
  }, { timeout: 120_000, intervals: [1_000, 2_000, 4_000] }).toContain('fail:');

  expect(detail?.spans).toHaveLength(4);
  await expect(page.getByRole('heading', { name: 'Behavior issue found' })).toBeVisible({ timeout: 60_000 });
  const issue = page.getByRole('article', { name: 'Evaluation result' }).filter({ hasText: 'fail' });
  await expect(issue).toContainText('fail');
  await expect(issue).toContainText(/eligib/i);
  const evaluatorScore = detail?.scores?.find((score) => score.source === 'evaluator' && score.string_value === 'fail');
  expect(evaluatorScore?.span_id, 'online evaluator result must be attached to an ingested span').toBeTruthy();
  expect(evaluatorScore?.config_id).toMatch(/^evaluator:.+:v1$/);
  await page.getByRole('button', { name: 'Open trace in Sessions' }).click();
  await expect(page.getByRole('heading', { name: `Session ${sessionId}` })).toBeVisible();
  const scoredSpan = detail!.spans!.find((item) => item.span_id === evaluatorScore!.span_id);
  expect(scoredSpan).toBeTruthy();
  await page.getByRole('button', { name: new RegExp(scoredSpan!.name) }).click();
  await expect(page.locator('.tle-eval-fail')).toBeVisible();

  const evaluatorId = evaluatorScore!.config_id!.replace(/^evaluator:/, '').replace(/:v1$/, '');
  const evaluatorsUrl = `${backend}/v1/evaluators`;
  const evaluatorState = async () => {
    const response = await request.get(evaluatorsUrl);
    expect(response.ok()).toBeTruthy();
    const definitions = await response.json() as Array<{ evaluator_id: string; active: boolean }>;
    return definitions.find((item) => item.evaluator_id === evaluatorId)?.active;
  };
  expect(await evaluatorState()).toBe(true);
  await page.getByRole('button', { name: 'Chat', exact: true }).click();
  await page.getByRole('button', { name: /Pause Before issuing a refund/ }).click();
  await expect.poll(evaluatorState).toBe(false);
  const reactivate = page.getByRole('button', { name: /Activate Before issuing a refund/ });
  await reactivate.click();
  await expect(page.getByRole('heading', { name: 'Reactivate behavior check' })).toBeVisible();
  const reactivationConsent = page.getByRole('checkbox', { name: 'I understand and want to activate this check.' });
  const reactivateButton = page.getByRole('button', { name: 'Activate check' });
  await expect(reactivateButton).toBeDisabled();
  await reactivationConsent.check();
  await reactivateButton.click();
  await expect.poll(evaluatorState).toBe(true);
});

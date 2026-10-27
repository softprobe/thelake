import { expect, test, type Page } from '@playwright/test';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

const backend = process.env.THELAKE_E2E_URL ?? 'http://127.0.0.1:18090';
const lisaBackend = process.env.THELAKE_LISA_E2E_URL ?? backend;
const suffix = `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
test.use({ video: 'on' });

test('Lisa flags one run immediately, recommends a rule from repeated runs, and reports later findings', async ({ page }) => {
  test.setTimeout(120_000);
  page.on('pageerror', (error) => console.error(`Lisa story page error: ${error.stack ?? error.message}`));
  page.on('requestfailed', (request) => console.error(`Lisa story request failed: ${request.url()} ${request.failure()?.errorText}`));
  await page.setViewportSize({ width: 1600, height: 1000 });
  const createdAt = new Date().toISOString();
  const userReactionAt = new Date(Date.parse(createdAt) + 1000).toISOString();
  const userReaction = {
    id: 'user-reacts-to-run', role: 'user',
    text: 'I see what you mean. Can you show me what led to that action?', created_at: userReactionAt,
  };
  const observationMessage = 'I noticed the agent permanently deleted the account without first asking the user to confirm. I can’t tell from this run whether the user intended that action, so I’m flagging it for your review.';
  const findingMessage = 'I checked a new run against the active policy. The account was deleted without asking the user to confirm first.';
  let status: 'pending' | 'approved' | 'dismissed' = 'pending';
  let recommendationVisible = false;
  let findingVisible = false;
  const observation = {
    id: 'observation-story', session_id: 'synthetic-account-17', activity_id: 'activity-run-17',
    agent_name: 'account-agent',
    summary: 'The agent permanently deleted account acct-demo-17 without asking the user to confirm.',
    span_ids: ['span-request-17', 'span-delete-17', 'span-result-17'],
    evidence: [
      { span_id: 'span-request-17', kind: 'user_message', name: 'message', content: 'I need help with my account.' },
      { span_id: 'span-delete-17', kind: 'tool_call', name: 'delete_account', content: 'delete_account(account_id="acct-demo-17")' },
      { span_id: 'span-result-17', kind: 'tool_result', name: 'delete_account', status: 'ok', content: '{"status":"deleted","account_id":"acct-demo-17"}' },
    ],
    created_at: createdAt,
  };
  const recommendation = {
    id: 'suggestion-story', name: 'Confirm before deleting an account',
    criteria: 'Before permanently deleting an account, confirm that the user explicitly requested deletion.',
    scope: 'agent', target_agent_name: 'account-agent',
    rationale: 'The same missing confirmation appeared in three separate runs.',
    signal_count: 3, overlaps: [], status, policy_id: status === 'approved' ? 'P1.1' : null,
    created_at: createdAt, reviewed_at: status === 'approved' ? createdAt : null,
    evidence: [1, 2, 3].map((run) => {
      const sessionId = `synthetic-account-${run}`;
      const spans = [
        { span_id: `span-agent-${run}`, kind: 'agent', name: 'account-agent', content: 'Started an account support run.' },
        { span_id: `span-request-${run}`, kind: 'user_input', name: 'user_message', content: 'I need help with my account.' },
        { span_id: `span-delete-${run}`, kind: 'tool', name: 'delete_account', content: `delete_account(account_id="acct-demo-${run}")` },
        { span_id: `span-result-${run}`, kind: 'tool_result', name: 'delete_account', content: '{"status":"deleted"}' },
      ];
      return {
        signal_id: `signal-${run}`, session_id: sessionId, activity_id: 'discovery-activity', agent_name: 'account-agent',
        summary: `Run ${run}: the account was permanently deleted without confirming the user's intent.`,
        span_ids: spans.map((item) => item.span_id),
        evidence: spans.map((item) => ({ ...item, session_id: sessionId, activity_id: 'discovery-activity', agent_name: 'account-agent' })),
      };
    }),
  };
  const updates = () => [
    { id: 'update-observation', source_type: 'qa_observation', source_id: 'observation-story', messages: [observationMessage], activity_id: 'activity-run-17', created_at: createdAt },
    ...(recommendationVisible ? [{ id: 'update-recommendation', source_type: 'policy_suggestion', source_id: 'suggestion-story', messages: [recommendation.rationale], activity_id: null, created_at: new Date(Date.parse(createdAt) + 60_000).toISOString() }] : []),
    ...(findingVisible ? [{ id: 'update-finding', source_type: 'policy_finding', source_id: 'evaluation-story', messages: [findingMessage], activity_id: 'evaluation-story', created_at: new Date(Date.parse(createdAt) + 120_000).toISOString() }] : []),
  ];
  await page.addInitScript(() => Object.defineProperty(window, '__LISA_API_BASE_PATH__', { value: '/lisa/v1' }));
  await page.route('**/lisa/v1/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === '/lisa/v1/conversations') {
      await route.fulfill({ json: [{ conversation_id: 'story-chat', title: 'Lisa', messages: [userReaction], activities: [] }] }); return;
    }
    if (url.pathname === '/lisa/v1/conversations/story-chat') {
      await route.fulfill({ json: { conversation_id: 'story-chat', title: 'Lisa', messages: [userReaction], activities: [] } }); return;
    }
    if (url.pathname === '/lisa/v1/proactive-updates') { await route.fulfill({ json: { items: updates() } }); return; }
    if (url.pathname === '/lisa/v1/qa-observations/observation-story') { await route.fulfill({ json: observation }); return; }
    if (url.pathname === '/lisa/v1/policy-suggestions') {
      const rows = recommendationVisible && (status === url.searchParams.get('status') || (url.searchParams.get('status') === 'pending' && status === 'pending')) ? [{ ...recommendation, status, policy_id: status === 'approved' ? 'P1.1' : null }] : [];
      await route.fulfill({ json: { items: rows, can_review: true } }); return;
    }
    if (url.pathname === '/lisa/v1/policy-suggestions/suggestion-story/approve') {
      status = 'approved'; await route.fulfill({ json: { ...recommendation, status, policy_id: 'P1.1' } }); return;
    }
    if (url.pathname === '/lisa/v1/policy-suggestions/suggestion-story/dismiss') {
      status = 'dismissed'; await route.fulfill({ json: { ...recommendation, status } }); return;
    }
    if (url.pathname === '/lisa/v1/policies') { await route.fulfill({ json: { items: status === 'approved' ? [{ id: 'P1.1', name: recommendation.name, criteria: recommendation.criteria, approved: true }] : [], next_offset: null } }); return; }
    if (url.pathname === '/lisa/v1/activities/evaluation-story') {
      await route.fulfill({ json: { activity: { id: 'evaluation-story', title: 'New account run · policy evaluation', kind: 'session_evaluation', status: 'success', status_title: 'Evaluation complete', summary: 'Policy violation found: explicit confirmation before deletion.', created_at: createdAt }, thread: { name: 'New account run · policy evaluation', status: 'success' }, actions: [], references: [{ reference_type: 'session', reference_id: 'synthetic-account-4' }] } }); return;
    }
    if (url.pathname === '/lisa/v1/activities/activity-run-17') {
      await route.fulfill({ json: { activity: { id: 'activity-run-17', title: 'Account run · QA review', kind: 'session_evaluation', status: 'success', status_title: 'Run reviewed', summary: observation.summary, created_at: createdAt }, thread: { name: 'Account run · QA review', status: 'success' }, actions: [], references: [{ reference_type: 'session', reference_id: observation.session_id }] } }); return;
    }
    if (url.pathname === '/lisa/v1/activities') { await route.fulfill({ json: [] }); return; }
    await route.fulfill({ status: 404, json: { detail: 'Unknown Lisa story fixture route' } });
  });

  await page.goto(`${lisaBackend}/lisa/`);
  const chat = page.getByRole('log', { name: 'Lisa conversation' });
  const captureDir = resolve(process.cwd(), '../../../../target/lisa-policy-story');
  await expect(chat.getByText(userReaction.text)).toBeVisible();
  await expect(chat.getByText(observationMessage)).toBeVisible();
  await expect(chat.getByRole('button', { name: 'Open run details' })).toBeVisible();
  await expect(chat.getByRole('button', { name: 'Ask Lisa about this' })).toHaveCount(0);
  await expect(chat.getByRole('button', { name: 'Discuss with Lisa' })).toHaveCount(0);
  const reactionBounds = await chat.getByText(userReaction.text).boundingBox();
  const observationBounds = await chat.locator('.tle-lisa-proactive-message').first().boundingBox();
  if (!reactionBounds || !observationBounds || observationBounds.y >= reactionBounds.y) {
    throw new Error('Lisa should surface the sampled-run observation before the owner responds');
  }
  const chatCrop = {
    x: Math.max(0, Math.floor(Math.min(reactionBounds.x, observationBounds.x) - 24)),
    y: Math.max(0, Math.floor(Math.min(reactionBounds.y, observationBounds.y) - 24)),
    width: Math.ceil(Math.max(reactionBounds.x + reactionBounds.width, observationBounds.x + observationBounds.width) - Math.min(reactionBounds.x, observationBounds.x) + 48),
    height: Math.ceil(Math.max(reactionBounds.y + reactionBounds.height, observationBounds.y + observationBounds.height) - Math.min(reactionBounds.y, observationBounds.y) + 48),
  };
  await page.screenshot({ path: resolve(captureDir, '05-chat-single-run-observation.png'), clip: chatCrop });
  await page.screenshot({ path: resolve(captureDir, '05-chat-single-run-observation-full.png'), fullPage: true });
  await chat.locator('.tle-lisa-proactive-message').first().screenshot({ path: resolve(captureDir, '06-chat-lisa-observation.png') });

  await page.waitForTimeout(11_000);
  recommendationVisible = true;
  await page.waitForTimeout(11_000);
  await expect(chat.getByText('The same missing confirmation appeared in three separate runs.')).toBeVisible();
  await chat.locator('.tle-lisa-proactive-message').nth(1).screenshot({ path: resolve(captureDir, '07-chat-recommendation-detail.png') });
  await page.screenshot({ path: resolve(captureDir, '07-chat-recommendation.png'), fullPage: true });
  await page.waitForTimeout(11_000);
  await chat.getByRole('button', { name: 'Approve policy' }).click();
  await expect(chat.getByText('Active policy · P1.1')).toBeVisible();
  await expect(chat.getByText('You approved this policy. Lisa will keep checking new runs against it.')).toBeVisible();
  await chat.locator('.tle-lisa-proactive-message').nth(1).screenshot({ path: resolve(captureDir, '08-chat-approved-policy-detail.png') });
  await page.screenshot({ path: resolve(captureDir, '08-chat-approved-policy.png'), fullPage: true });
  await page.waitForTimeout(11_000);
  findingVisible = true;
  await expect(chat.getByText(findingMessage)).toBeVisible();
  await expect(chat.getByRole('button', { name: 'Open evaluation evidence' })).toBeVisible();
  await chat.locator('.tle-lisa-proactive-message').nth(2).screenshot({ path: resolve(captureDir, '09-chat-later-finding-detail.png') });
  await page.screenshot({ path: resolve(captureDir, '09-chat-later-finding.png'), fullPage: true });
  await page.waitForTimeout(11_000);
  const video = page.video();
  await page.close();
  if (video) await video.saveAs(resolve(captureDir, 'lisa-proactive-policy-story.webm'));
});

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

async function sendLisaMessage(page: Page, text: string) {
  const composer = page.getByRole('textbox', { name: 'Message Lisa' });
  await composer.fill(text);
  const responsePromise = page.waitForResponse((response) =>
    response.request().method() === 'POST' && /\/v1\/conversations\/[^/]+\/messages$/.test(response.url()),
    { timeout: 240_000 },
  );
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const response = await responsePromise;
  expect(response.ok()).toBeTruthy();
  await response.finished();
  await expect(page.locator('.tle-lisa-working')).toHaveCount(0, { timeout: 30_000 });
  await expect(composer).toHaveValue('');
}

test('live thelake ingest, reducer, session filters, and trace detail render in Explorer', async ({ page, request }) => {
  test.setTimeout(120_000);
  page.on('pageerror', (error) => console.error(`Browser page error: ${error.stack ?? error.message}`));
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
      span({
        traceId, spanId: agentSpanId, sessionId, name: agent, type: 'agent', start,
        attributes: { 'sp.agent.name': agent },
      }),
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

  await page.addInitScript(() => {
    Object.defineProperty(window, '__LISA_API_BASE_PATH__', { value: '/lisa/v1' });
  });
  const longSqlMessage = {
    id: 'assistant-code-seed', role: 'assistant',
    text: `Here is the query:\n\n\`\`\`sql\nSELECT ${Array.from({ length: 48 }, (_, index) => `policy_column_${index}`).join(', ')} FROM lisa.readable_evaluations\n\`\`\``,
    created_at: new Date().toISOString(),
  };
  let failNextActivityFeedRequest = false;
  await page.route('**/lisa/v1/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === '/lisa/v1/conversations') {
      await route.fulfill({ json: [{ conversation_id: 'lisa-ui-seed', title: 'Lisa demo', messages: [longSqlMessage], activities: [] }] });
      return;
    }
    if (url.pathname === '/lisa/v1/conversations/lisa-ui-seed') {
      await route.fulfill({ json: { conversation_id: 'lisa-ui-seed', title: 'Lisa demo', messages: [longSqlMessage], activities: [] } });
      return;
    }
    if (url.pathname === '/lisa/v1/activities') {
      const activityType = url.searchParams.get('activity_type');
      if (!activityType && failNextActivityFeedRequest) {
        failNextActivityFeedRequest = false;
        await route.fulfill({ status: 503, json: { detail: 'Temporary preview feed error' } });
        return;
      }
      if (!activityType) await new Promise((resolve) => setTimeout(resolve, 800));
      const rows = activityType === 'policy_evaluation' ? [{
        id: 'evaluation-seed', title: 'Evaluation from the policy dataset', kind: 'policy_evaluation',
        status: 'success', status_title: 'Evaluation complete', summary: 'Triage escalated this case to the judge.',
        created_at: new Date().toISOString(), updated_at: new Date().toISOString(),
      }] : activityType === 'session_evaluation' ? [{
        id: 'session-evaluation-seed', title: 'Evaluation from a pushed session', kind: 'session_evaluation',
        status: 'success', status_title: 'Evaluation complete', summary: 'Policy violation found: Refund eligibility.',
        created_at: new Date().toISOString(), updated_at: new Date().toISOString(),
      }] : [{
        id: 'activity-seed', title: 'Review a seeded activity', kind: 'action',
        status: 'pending', status_title: 'In progress', summary: 'Activity fixture for the Lisa panel.',
        created_at: new Date().toISOString(), updated_at: new Date().toISOString(),
      }];
      await route.fulfill({ json: rows });
      return;
    }
    if (url.pathname === '/lisa/v1/activities/activity-seed') {
      await new Promise((resolve) => setTimeout(resolve, 800));
      const createdAt = new Date().toISOString();
      await route.fulfill({ json: {
        activity: { id: 'activity-seed', title: 'Review a seeded activity', status: 'pending', status_title: 'In progress', summary: 'Activity fixture for the Lisa panel.' },
        thread: { name: 'Review a seeded activity', status: 'pending' }, actions: [
          { seq: 1, event_type: 'activity.started', title: 'Started', summary: 'Lisa began reviewing the fixture.', created_at: createdAt },
          { seq: 2, event_type: 'activity.updated', title: 'Checked workspace evidence', summary: 'The activity detail shows the selected timeline step.', created_at: createdAt },
        ],
        references: [
          { reference_type: 'message', reference_id: 'message-seed' },
          { reference_type: 'runtime_event', reference_id: 'event-seed' },
        ],
      } });
      return;
    }
    if (url.pathname === '/lisa/v1/policies') {
      await route.fulfill({ json: { items: [{
        id: 'P1.1', scope: 'workspace', name: 'Refund eligibility before refund',
        criteria: 'Check refund eligibility before issuing a refund.', approved: true,
      }], next_offset: null } });
      return;
    }
    await route.fulfill({ status: 404, json: { detail: 'Unknown Lisa UI test route' } });
  });

  await page.goto('/explorer/');
  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Lisa' })).toHaveAttribute('href', '/lisa/');

  await page.goto(`${lisaBackend}/lisa/`);
  const lisaChat = page.getByRole('log', { name: 'Lisa conversation' });
  await expect(lisaChat).toBeVisible();
  const codeMessage = page.locator('.tle-lisa-message.tle-lisa-assistant').filter({ has: page.locator('.tle-lisa-markdown pre') }).first();
  const codeBlock = codeMessage.locator('.tle-lisa-markdown pre');
  await expect(codeBlock).toBeVisible();
  const codeBlockBounds = await page.evaluate(() => {
    const message = document.querySelector('.tle-lisa-message.tle-lisa-assistant')!;
    const pre = message.querySelector('.tle-lisa-markdown pre')!;
    const transcript = document.querySelector('.tle-lisa-transcript')!;
    return {
      messageRight: message.getBoundingClientRect().right,
      preRight: pre.getBoundingClientRect().right,
      transcriptRight: transcript.getBoundingClientRect().right,
      preScrollWidth: pre.scrollWidth,
      preClientWidth: pre.clientWidth,
    };
  });
  expect(codeBlockBounds.preRight).toBeLessThanOrEqual(codeBlockBounds.messageRight + 1);
  expect(codeBlockBounds.messageRight).toBeLessThanOrEqual(codeBlockBounds.transcriptRight + 1);
  expect(codeBlockBounds.preScrollWidth).toBeGreaterThan(codeBlockBounds.preClientWidth);
  for (const tab of ['Activities', 'Identity']) {
    await expect(page.getByRole('tab', { name: tab })).toBeVisible();
  }
  const activityTab = page.getByRole('tab', { name: 'Activities' });
  await activityTab.focus();
  await activityTab.press('ArrowRight');
  await expect(page.getByRole('tab', { name: 'Identity' })).toBeFocused();
  await expect(page.getByRole('tab', { name: 'Identity' })).toHaveAttribute('aria-selected', 'true');
  await page.getByRole('tab', { name: 'Identity' }).press('ArrowLeft');
  await expect(activityTab).toBeFocused();
  await page.getByRole('button', { name: 'Findings' }).click();
  const finding = page.getByRole('button', { name: /Evaluation from a pushed session/ });
  await expect(finding).toBeVisible();
  await expect(finding).toContainText('Policy violations found');
  await expect(finding).toContainText('Refund eligibility.');
  await expect(page.getByRole('button', { name: /Evaluation from the policy dataset/ })).toHaveCount(0);
  await page.getByRole('button', { name: 'Evaluations' }).click();
  await expect(page.getByRole('button', { name: /Evaluation from the policy dataset/ })).toBeVisible();
  const pushedEvaluation = page.getByRole('button', { name: /Evaluation from a pushed session/ });
  await expect(pushedEvaluation).toBeVisible();
  await expect(pushedEvaluation).toContainText('Policy violations found');
  await expect(pushedEvaluation).toContainText('Refund eligibility.');
  await page.waitForTimeout(900);
  await expect(page.getByRole('button', { name: /Evaluation from the policy dataset/ })).toBeVisible();
  await expect(pushedEvaluation).toContainText('Policy violations found');
  await activityTab.click();
  await expect(page.getByRole('button', { name: /Review a seeded activity/ })).toBeVisible();
  const detailRequest = page.waitForRequest((request) => new URL(request.url()).pathname.endsWith('/activities/activity-seed'));
  await page.getByRole('button', { name: /Review a seeded activity/ }).click();
  await detailRequest;
  await page.getByRole('button', { name: 'Evaluations' }).click();
  await page.waitForTimeout(900);
  await expect(page.getByRole('dialog', { name: 'Review a seeded activity' })).toHaveCount(0);
  failNextActivityFeedRequest = true;
  await activityTab.click();
  await expect(page.getByRole('alert')).toContainText('activity feed could not load');
  await activityTab.click();
  await expect(page.getByText('Loading activities…')).toBeVisible();
  await expect(page.getByText('Loading activities…')).toHaveCount(0);
  await expect(page.getByRole('alert')).toHaveCount(0);
  await page.getByRole('button', { name: /Review a seeded activity/ }).click();
  const activityDialog = page.getByRole('dialog', { name: 'Review a seeded activity' });
  await expect(activityDialog).toBeVisible();
  await expect(activityDialog.getByRole('heading', { name: 'Summary' })).toBeVisible();
  await expect(activityDialog.getByText('Activity fixture for the Lisa panel.')).toBeVisible();
  await expect(activityDialog.locator('[aria-current="step"]')).toHaveCount(0);
  await expect(activityDialog.locator('.tle-lisa-activity-step.latest')).toHaveCount(1);
  await activityDialog.getByRole('button', { name: 'Checked workspace evidence' }).click();
  await expect(activityDialog.getByRole('heading', { name: 'Checked workspace evidence' })).toBeVisible();
  await expect(activityDialog.getByText('The activity detail shows the selected timeline step.')).toBeVisible();
  await expect(activityDialog.getByRole('button', { name: 'Checked workspace evidence' })).toHaveAttribute('aria-current', 'step');
  const linkedEvidence = activityDialog.getByRole('region', { name: 'Linked evidence' });
  await expect(linkedEvidence.getByText('message-seed')).toBeVisible();
  await expect(linkedEvidence.getByText('event-seed')).toBeVisible();
  await activityDialog.getByRole('button', { name: 'Started' }).click();
  await expect(activityDialog.getByRole('heading', { name: 'Started' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(activityDialog).toHaveCount(0);
  await expect(page.getByRole('button', { name: /Review a seeded activity/ })).toBeFocused();
  await page.getByRole('button', { name: 'Policies' }).click();
  await expect(page.getByText('Refund eligibility before refund', { exact: true })).toBeVisible();
  await page.getByRole('tab', { name: 'Identity' }).click();
  await expect(page.getByText('SOUL.md', { exact: true })).toBeVisible();
  const chatPanel = page.locator('.tle-lisa-main');
  const panelOpenWidth = (await chatPanel.boundingBox())!.width;
  await page.getByRole('button', { name: 'Close workspace panel' }).click();
  await expect(page.getByRole('complementary', { name: 'Lisa workspace panel' })).toHaveCount(0);
  await expect.poll(async () => (await chatPanel.boundingBox())!.width).toBeGreaterThan(panelOpenWidth + 300);
  await page.getByRole('button', { name: 'Show workspace panel' }).click();
  await expect(page.getByRole('complementary', { name: 'Lisa workspace panel' })).toBeVisible();
  await expect.poll(async () => (await chatPanel.boundingBox())!.width).toBe(panelOpenWidth);
  await expect(page.getByText('MEMORY.md', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Back to Lisa' }).click();
  await expect(lisaChat).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: 'Close workspace panel' }).click();
  await expect.poll(async () => (await chatPanel.boundingBox())!.height).toBeGreaterThan(800);
  await expect(page.locator('.tle-lisa-rail')).toHaveCSS('grid-row-end', '2');
  await page.getByRole('button', { name: 'Show workspace panel' }).click();
  await expect.poll(async () => (await chatPanel.boundingBox())!.height).toBeLessThan(600);
  await page.setViewportSize({ width: 1280, height: 900 });

  await page.getByRole('button', { name: 'Sessions', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();

  const range = page.getByRole('combobox', { name: 'Session time range' });
  const sessionLink = (sessionId: string) => page.locator(`a[href="?session=${sessionId}"]`);
  await range.selectOption('1');
  await expect(sessionLink(sessions.alpha)).toBeVisible();
  await expect(sessionLink(sessions.beta)).toBeVisible();
  await expect(sessionLink(sessions.old)).toHaveCount(0);
  await range.selectOption('7');
  await expect(sessionLink(sessions.old)).toBeVisible();

  await page.getByRole('button', { name: 'Filter' }).click();
  const agentFilter = page.getByRole('combobox', { name: 'Filter Sessions by Agent' });
  await agentFilter.selectOption({ label: 'agent-beta-e2e' });
  await expect(sessionLink(sessions.beta)).toBeVisible();
  await expect(sessionLink(sessions.alpha)).toHaveCount(0);
  await expect(sessionLink(sessions.old)).toHaveCount(0);

  await page.getByRole('button', { name: 'Clear' }).click();
  const alphaLink = sessionLink(sessions.alpha);
  const alphaRow = page.locator('tr').filter({ has: alphaLink });
  await expect(alphaRow.getByRole('cell').nth(3)).toContainText('3 nodes');
  await expect(alphaRow.getByRole('cell').nth(4)).toHaveText('—');
  await expect(page.getByRole('columnheader', { name: 'FINDING' })).toBeVisible();
  await alphaLink.click();

  await expect(page.getByRole('heading', { name: `Session ${sessions.alpha}` })).toBeVisible();
  await expect(page).toHaveURL(new RegExp(`session=${sessions.alpha}`));
  await page.goBack();
  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
  await page.goForward();
  await expect(page.getByRole('heading', { name: `Session ${sessions.alpha}` })).toBeVisible();
  await openDetails(page, 'Session details');
  await expect(page.getByText('Trace ID', { exact: true })).toBeVisible();
  const spanTimeline = page.getByRole('region', { name: 'Session traces and spans' });
  await expect(page.getByRole('tab', { name: 'Trajectory' })).toHaveAttribute('aria-selected', 'true');
  await expect(spanTimeline.getByRole('button', { name: /lookup_customer/ })).toBeVisible();
  await expect(spanTimeline.locator('.trajectory-node-select').filter({ hasText: 'LLM call' })).toBeVisible();
  await expect(spanTimeline.getByText('Decide which plan applies to this customer')).toBeVisible();
  await page.getByRole('tab', { name: 'Trace' }).click();
  await expect(page.getByRole('tab', { name: 'Trace' })).toHaveAttribute('aria-selected', 'true');
  await expect(spanTimeline.getByRole('button', { name: /reasoning/ })).toBeVisible();
  await page.getByRole('tab', { name: 'Evidence' }).click();
  await expect(page.getByRole('tab', { name: 'Evidence' })).toHaveAttribute('aria-selected', 'true');
  await expect(spanTimeline.getByText('Finding evidence is unavailable')).toBeVisible();
  await page.getByRole('tab', { name: 'Trajectory' }).click();
  await spanTimeline.getByRole('textbox', { name: 'Search spans' }).fill('lookup_customer');
  await expect(spanTimeline.getByRole('button', { name: /reasoning/ })).toHaveCount(0);
  await spanTimeline.getByRole('textbox', { name: 'Search spans' }).fill('');
  await spanTimeline.getByRole('button', { name: /lookup_customer/ }).click();
  await expect(page.locator('.payload-grid').getByText('{"customer_id":"cust-e2e-42"}', { exact: true })).toBeVisible();
  await openDetails(page, 'Attributes and events');
  await expect(page.getByText('tool.started')).toBeVisible();
  await spanTimeline.locator('.trajectory-node-select').filter({ hasText: 'LLM call' }).click();
  await openDetails(page, 'Attributes and events');
  await expect(page.getByText('Matched the active subscription.')).toBeVisible();
  await expect(page.locator('.payload-grid').getByText('The customer is on the pro plan.', { exact: true })).toBeVisible();
});

test('Lisa creates, finds, approves, and updates a policy through standalone chat', async ({ page }) => {
  test.skip(process.env.THELAKE_EXPLORER_E2E_ONLINE !== '1', 'requires the real policy QA stack');
  test.setTimeout(900_000);
  const agentName = `refund-agent-e2e-${suffix}`;
  const policyName = `Refund eligibility ${suffix}`;
  await page.goto(`${lisaBackend}/lisa/`);
  await expect(page.getByRole('log', { name: 'Lisa conversation' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Activate', exact: true })).toHaveCount(0);
  await expect(page.getByText('Connected', { exact: true })).toBeVisible();
  await expect(page.getByText(/Messages and recent workspace evidence are sent to your configured model provider/)).toBeVisible();
  await expect(page.getByText('No activities yet.')).toBeVisible();
  const composer = page.getByRole('textbox', { name: 'Message Lisa' });
  await expect(composer).toBeEnabled({ timeout: 30_000 });

  await sendLisaMessage(page, `Create an unapproved agent policy named "${policyName}" for "${agentName}": ` +
    'check refund eligibility and wait for the result before issuing a refund. ' +
    'Use write_policy and show me its readable policy ID. Do not approve it yet.');
  const transcript = await page.getByRole('log', { name: 'Lisa conversation' }).innerText();
  const policyId = transcript.match(/\bP1\.\d+(?:\.\d+)?\b/)?.[0];
  expect(policyId).toBeTruthy();
  await expect(page.locator('.tle-lisa-activity-card').first()).toBeVisible({ timeout: 30_000 });
  const activityDialog = page.locator('.tle-lisa-activity-dialog');
  await page.locator('.tle-lisa-activity-card').first().click();
  await expect(activityDialog).toBeVisible();
  const activityTimeline = activityDialog.getByRole('navigation', { name: 'Activity timeline' });
  await activityTimeline.getByRole('button', { name: /You: Create an unapproved agent policy/ }).click();
  await expect(activityDialog.locator('.tle-lisa-activity-dialog-summary p').first())
    .toContainText(/Create an unapproved agent policy/);
  await activityTimeline.getByRole('button', { name: /Lisa used write_policy/ }).click();
  await expect(activityDialog.locator('.tle-lisa-activity-dialog-summary p').first())
    .toContainText(policyName);
  await activityDialog.getByRole('button', { name: 'Close activity details' }).click();

  await sendLisaMessage(page, `Find the policy named "${policyName}" for "${agentName}". ` +
    'Use list_policies and read_policy, and report its readable ID, criteria, and approval state.');
  let policyTranscript = await page.getByRole('log', { name: 'Lisa conversation' }).innerText();
  expect(policyTranscript).toContain(policyId!);
  expect(policyTranscript).toMatch(/approved: no|not approved/i);

  await sendLisaMessage(page, `Approve policy: ${policyId}`);
  await expect(page.locator('.tle-lisa-tool summary').filter({ hasText: 'Approve policy' })).toBeVisible();
  await expect(page.locator('.tle-lisa-activity-card').first()).toBeVisible({ timeout: 30_000 });

  await sendLisaMessage(page, `Update the approved policy ${policyId} named "${policyName}" for "${agentName}". ` +
    'Add the requirement to record the customer\'s explicit confirmation before issuing a refund. ' +
    'Use write_policy to update this policy in place, leave it unapproved, and show its unchanged ID.');
  await expect(page.getByRole('log', { name: 'Lisa conversation' })).toContainText(policyId!, { timeout: 240_000 });
  await expect(page.locator('.tle-lisa-activity-card').first()).toBeVisible({ timeout: 30_000 });
  await page.locator('.tle-lisa-activity-card').first().click();
  await expect(activityDialog).toBeVisible();
  const updateTimeline = activityDialog.getByRole('navigation', { name: 'Activity timeline' });
  await updateTimeline.getByRole('button', { name: /You: Update the approved policy/ }).click();
  await expect(activityDialog.locator('.tle-lisa-activity-dialog-summary p').first())
    .toContainText(/Update the approved policy/);
  await updateTimeline.getByRole('button', { name: /Lisa used write_policy/ }).click();
  await expect(activityDialog.locator('.tle-lisa-activity-dialog-summary p').first())
    .toContainText(policyId!);
  await activityDialog.getByRole('button', { name: 'Close activity details' }).click();
  await sendLisaMessage(page, `Find the policy named "${policyName}" for "${agentName}" again. ` +
    'Use list_policies and read_policy, and report its exact ID and approval state.');
  policyTranscript = await page.getByRole('log', { name: 'Lisa conversation' }).innerText();
  expect(policyTranscript).toContain(policyId!);
  expect(policyTranscript).toMatch(/approved: no|not approved/i);
});

test('Lisa deletes an existing agent policy from chat and confirms its absence', async ({ page }) => {
  test.skip(process.env.THELAKE_EXPLORER_E2E_ONLINE !== '1', 'requires the real Lisa runtime');
  test.setTimeout(300_000);
  const agentName = `delete-policy-agent-${suffix}`;
  const policyName = `Delete policy browser check ${suffix}`;
  await page.goto(`${lisaBackend}/lisa/`);
  await expect(page.getByRole('log', { name: 'Lisa conversation' })).toBeVisible();
  await expect(page.getByText('Connected', { exact: true })).toBeVisible();
  await expect(page.getByRole('textbox', { name: 'Message Lisa' })).toBeEnabled({ timeout: 30_000 });

  await sendLisaMessage(page, `Create an unapproved agent policy named "${policyName}" for "${agentName}" with criterion "test-only deletion target". Leave it pending and tell me its policy ID.`);
  const createTool = page.locator('.tle-lisa-tool').filter({ has: page.locator('summary').filter({ hasText: 'Write policy' }) }).last();
  await expect(createTool).toBeVisible();
  await createTool.locator('summary').click();
  const policyId = (await createTool.locator('pre').last().innerText()).match(/"policy_id":\s*"([^"]+)"/)?.[1];
  expect(policyId).toMatch(/^P1\.\d+(?:\.\d+)*$/);

  await sendLisaMessage(page, `Delete policy ${policyId} for agent "${agentName}".`);
  const deleteTool = page.locator('.tle-lisa-tool').filter({ has: page.locator('summary').filter({ hasText: 'Delete policy' }) }).last();
  await expect(deleteTool).toBeVisible();
  await deleteTool.locator('summary').click();
  await expect(deleteTool.locator('pre').first()).toContainText(`"policy_id": "${policyId}"`);
  await expect(deleteTool.locator('pre').last()).toHaveText(/"deleted"\s*:\s*true/);

  await sendLisaMessage(page, `Use read_policy with scope=agent and agent_name="${agentName}" to verify whether policy ${policyId} remains.`);
  const readTool = page.locator('.tle-lisa-tool').filter({ has: page.locator('summary').filter({ hasText: 'Read policy' }) }).last();
  await expect(readTool).toBeVisible();
  await readTool.locator('summary').click();
  await expect(readTool.locator('pre').first()).toContainText('"scope": "agent"');
  await expect(readTool.locator('pre').first()).toContainText(`"agent_name": "${agentName}"`);
  await expect(readTool.locator('pre').last()).toContainText('No policy exists yet.');
});

test('Lisa shows a retryable evaluation failure as the activity updates', async ({ page }) => {
  const activityId = 'retryable-evaluation-seed';
  const title = 'Evaluate a pushed session';
  const failureSummary = 'Evaluation attempt 1 of 8 failed: Judge timed out. Lisa will retry.';
  const createdAt = new Date().toISOString();
  let failureRecorded = false;

  await page.addInitScript(() => {
    Object.defineProperty(window, '__LISA_API_BASE_PATH__', { value: '/lisa/v1' });
  });
  await page.route('**/lisa/v1/**', async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === '/lisa/v1/conversations') {
      await route.fulfill({ json: [{ conversation_id: 'retry-seed', title: 'Lisa demo', messages: [], activities: [] }] });
      return;
    }
    if (url.pathname === '/lisa/v1/conversations/retry-seed') {
      await route.fulfill({ json: { conversation_id: 'retry-seed', title: 'Lisa demo', messages: [], activities: [] } });
      return;
    }
    if (url.pathname === '/lisa/v1/activities') {
      await route.fulfill({ json: [{
        id: activityId,
        title,
        kind: 'session_evaluation',
        status: 'pending',
        status_title: 'In progress',
        summary: failureRecorded ? failureSummary : 'Evaluation is queued.',
        created_at: createdAt,
        updated_at: createdAt,
      }] });
      return;
    }
    if (url.pathname === `/lisa/v1/activities/${activityId}`) {
      const actions = [
        { seq: 1, event_type: 'activity.started', title: 'Evaluation started', summary: 'Lisa started the evaluation.', created_at: createdAt },
      ];
      if (failureRecorded) {
        actions.push({
          seq: 2,
          event_type: 'session_evaluation_attempt_failed',
          title: 'Evaluation attempt 1 of 8 failed',
          summary: failureSummary,
          created_at: createdAt,
        });
      }
      await route.fulfill({ json: {
        activity: {
          id: activityId,
          title,
          status: 'pending',
          status_title: 'In progress',
          summary: failureRecorded ? failureSummary : 'Evaluation is queued.',
        },
        thread: { name: title, status: 'pending' },
        actions,
        references: [],
      } });
      return;
    }
    await route.fulfill({ status: 404, json: { detail: 'Unknown Lisa retry visibility test route' } });
  });

  await page.goto(`${lisaBackend}/lisa/`);
  const activityCard = page.getByRole('button', { name: new RegExp(title) });
  await expect(activityCard).toContainText('Evaluation is queued.');
  await activityCard.click();
  const dialog = page.getByRole('dialog', { name: title });
  await expect(dialog.getByText('Evaluation is queued.')).toBeVisible();

  failureRecorded = true;
  await expect(activityCard).toContainText(failureSummary, { timeout: 20_000 });
  const timeline = dialog.getByRole('navigation', { name: 'Activity timeline' });
  const failureStep = timeline.getByRole('button', { name: /Evaluation attempt 1 of 8 failed/ });
  await expect(failureStep).toBeVisible({ timeout: 20_000 });
  await expect(failureStep.locator('.tle-lisa-step-icon')).toHaveClass(/failed/);
  await failureStep.click();
  await expect(dialog.getByText(/Judge timed out\. Lisa will retry\./)).toBeVisible();
});

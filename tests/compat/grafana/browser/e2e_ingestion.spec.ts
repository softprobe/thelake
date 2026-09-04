import { test, expect } from '@playwright/test';

const SOFTPROBE_URL = process.env.SOFTPROBE_URL || 'http://127.0.0.1:8090';
const API_KEY = process.env.SOFTPROBE_API_KEY || 'local-dev-key';
const TENANT_ID = process.env.SOFTPROBE_TENANT_ID || 'local-dev-tenant';

test.describe('OpenTelemetry Demo Ingestion Pipeline (E2E)', () => {
  test('I-01: Softprobe runtime reports ready', async ({ request }) => {
    const resp = await request.get(`${SOFTPROBE_URL}/ready`);
    expect(resp.status()).toBe(200);
    const body = await resp.json();
    expect(body.status).toBe('ready');
  });

  test('I-02: OTel Demo metrics stream is active and fresh', async ({ request }) => {
    const end = Math.floor(Date.now() / 1000);
    const start = end - 900;

    const resp = await request.get(`${SOFTPROBE_URL}/api/v1/query_range`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
      params: {
        query: 'k6_http_reqs',
        start: start.toString(),
        end: end.toString(),
        step: '15',
      },
    });

    expect(resp.status()).toBe(200);
    const json = await resp.json();
    expect(json.status).toBe('success');
    const results = json.data?.result || [];
    expect(results.length).toBeGreaterThan(0);

    // Verify recent timestamps: the latest sample must be within the last 600s
    let latestTimestamp = 0;
    for (const series of results) {
      for (const [ts, _] of series.values || []) {
        if (ts > latestTimestamp) latestTimestamp = ts;
      }
    }
    const lagSeconds = end - latestTimestamp;
    expect(lagSeconds).toBeLessThan(600);
  });

  test('I-03: Continuous live scrapes show varying counter values', async ({ request }) => {
    const end = Math.floor(Date.now() / 1000);
    const start = end - 900;

    const resp = await request.get(`${SOFTPROBE_URL}/api/v1/query_range`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
      params: {
        query: 'k6_http_reqs',
        start: start.toString(),
        end: end.toString(),
        step: '15',
      },
    });

    expect(resp.status()).toBe(200);
    const json = await resp.json();
    const results = json.data?.result || [];
    expect(results.length).toBeGreaterThan(0);

    let maxChanges = 0;
    for (const series of results) {
      const vals = (series.values || []).map((pt: [number, string]) => parseFloat(pt[1]));
      let changes = 0;
      for (let i = 1; i < vals.length; i++) {
        if (vals[i] !== vals[i - 1]) changes++;
      }
      if (changes > maxChanges) maxChanges = changes;
    }

    expect(maxChanges).toBeGreaterThanOrEqual(1);
  });

  test('I-04: Multi-service metric coverage from Astronomy Shop demo', async ({ request }) => {
    const resp = await request.get(`${SOFTPROBE_URL}/api/v1/label/__name__/values`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
    });

    expect(resp.status()).toBe(200);
    const json = await resp.json();
    expect(json.status).toBe('success');
    const names: string[] = json.data || [];

    // Check for load generator metrics
    expect(names.some(n => n.startsWith('k6_'))).toBe(true);

    // Check for demo business metrics (ad, cart, etc.)
    expect(names.some(n => n.startsWith('demo_'))).toBe(true);

    // Check for HTTP server metrics
    expect(names.some(n => n.startsWith('http_server_'))).toBe(true);
  });

  test('I-05: Application logs stream from Astronomy Shop demo', async ({ request }) => {
    const endNs = BigInt(Date.now()) * BigInt(1_000_000);
    const startNs = endNs - BigInt(3600) * BigInt(1_000_000_000);

    const resp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/labels`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
      params: {
        start: startNs.toString(),
        end: endNs.toString(),
      },
    });

    expect(resp.status()).toBe(200);
    const json = await resp.json();
    expect(json.status).toBe('success');
    const labels: string[] = json.data || [];
    expect(labels).toContain('service_name');

    // Query values for service_name
    const valuesResp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/label/service_name/values`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
      params: {
        start: startNs.toString(),
        end: endNs.toString(),
      },
    });

    expect(valuesResp.status()).toBe(200);
    const valuesJson = await valuesResp.json();
    expect(valuesJson.status).toBe('success');
    expect(valuesJson.data?.length).toBeGreaterThan(0);
  });
});

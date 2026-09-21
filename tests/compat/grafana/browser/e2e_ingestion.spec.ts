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

  test('I-02: Application logs stream from Astronomy Shop demo', async ({ request }) => {
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

    const valuesResp = await request.get(
      `${SOFTPROBE_URL}/loki/api/v1/label/service_name/values`,
      {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
        },
        params: {
          start: startNs.toString(),
          end: endNs.toString(),
        },
      },
    );

    expect(valuesResp.status()).toBe(200);
    const valuesJson = await valuesResp.json();
    expect(valuesJson.status).toBe('success');
    expect(valuesJson.data?.length).toBeGreaterThan(0);
  });

  test('I-03: Tempo search returns a protocol envelope', async ({ request }) => {
    const resp = await request.get(`${SOFTPROBE_URL}/api/search`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        'X-Scope-OrgID': TENANT_ID,
      },
      params: { limit: '10' },
    });
    expect(resp.status()).toBe(200);
    const json = await resp.json();
    expect(Array.isArray(json.traces)).toBe(true);
  });
});

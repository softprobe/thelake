import { test, expect } from '@playwright/test';
import { QUERY_FEATURE_CATALOG } from './query_features';

const GRAFANA_URL = process.env.GRAFANA_URL || 'http://127.0.0.1:3000';
const SOFTPROBE_URL = process.env.SOFTPROBE_URL || 'http://127.0.0.1:8090';
const API_KEY = process.env.SOFTPROBE_API_KEY || 'local-dev-key';
const TENANT_ID = process.env.SOFTPROBE_TENANT_ID || 'local-dev-tenant';

const GRAFANA_AUTH_HEADERS = {
  Authorization: 'Basic ' + Buffer.from('admin:admin').toString('base64'),
};

test.describe('Query Features Verification (Loki + Tempo)', () => {
  test.describe('Loki LogQL Features', () => {
    for (const item of QUERY_FEATURE_CATALOG) {
      test(`[${item.id}] loki - ${item.name}: ${item.expr}`, async ({ request }) => {
        const endNs = BigInt(Date.now()) * BigInt(1_000_000);
        const startNs = endNs - BigInt(3600) * BigInt(1_000_000_000);

        const dsResp = await request.post(`${GRAFANA_URL}/api/ds/query`, {
          headers: {
            ...GRAFANA_AUTH_HEADERS,
            'Content-Type': 'application/json',
          },
          data: {
            queries: [
              {
                refId: 'A',
                datasource: { type: 'loki', uid: 'softprobe-loki-a' },
                expr: item.expr,
                queryType: 'range',
              },
            ],
            from: 'now-1h',
            to: 'now',
          },
        });
        expect(dsResp.status()).toBe(200);
        const dsJson = await dsResp.json();
        expect(dsJson.results?.A).toBeDefined();
        expect(dsJson.results?.A?.status ?? 200).toBe(200);

        const resp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/query_range`, {
          headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
          params: {
            query: item.expr,
            start: startNs.toString(),
            end: endNs.toString(),
            limit: '50',
          },
        });

        expect(resp.status()).toBe(200);
        const json = await resp.json();
        expect(json.status).toBe('success');
        expect(json.data?.resultType).toBe('streams');
        const streams = json.data?.result || [];
        if (item.validate) {
          expect(item.validate(streams)).toBe(true);
        }
      });
    }

    test('L-07: Loki labels and label values discovery', async ({ request }) => {
      const endNs = BigInt(Date.now()) * BigInt(1_000_000);
      const startNs = endNs - BigInt(3600) * BigInt(1_000_000_000);

      const labelsResp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/labels`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { start: startNs.toString(), end: endNs.toString() },
      });
      expect(labelsResp.status()).toBe(200);
      const labelsJson = await labelsResp.json();
      expect(labelsJson.status).toBe('success');
      expect(labelsJson.data).toContain('service_name');

      const valuesResp = await request.get(
        `${SOFTPROBE_URL}/loki/api/v1/label/service_name/values`,
        {
          headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
          params: { start: startNs.toString(), end: endNs.toString() },
        },
      );
      expect(valuesResp.status()).toBe(200);
      const valuesJson = await valuesResp.json();
      expect(valuesJson.status).toBe('success');
      expect(valuesJson.data.length).toBeGreaterThan(0);
    });
  });

  test.describe('Tempo Protocol Endpoints', () => {
    test('TR-01 & TR-02: Search and trace lookup protocol endpoints', async ({ request }) => {
      const searchResp = await request.get(`${SOFTPROBE_URL}/api/search`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { limit: '10' },
      });
      expect(searchResp.status()).toBe(200);
      const searchJson = await searchResp.json();
      expect(Array.isArray(searchJson.traces)).toBe(true);

      const lookupResp = await request.get(
        `${SOFTPROBE_URL}/api/traces/00000000000000000000000000000000`,
        {
          headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        },
      );
      expect(lookupResp.status()).toBe(404);
      const lookupJson = await lookupResp.json();
      expect(lookupJson.message).toBe('trace not found');
    });

    test('TR-03 & TR-04: Search tags and tag values protocol discovery', async ({ request }) => {
      const tagsResp = await request.get(`${SOFTPROBE_URL}/api/search/tags`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(tagsResp.status()).toBe(200);
      const tagsJson = await tagsResp.json();
      expect(Array.isArray(tagsJson.tagNames)).toBe(true);

      const tagValuesResp = await request.get(
        `${SOFTPROBE_URL}/api/search/tag/service.name/values`,
        {
          headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        },
      );
      expect(tagValuesResp.status()).toBe(200);
      const tagValuesJson = await tagValuesResp.json();
      expect(Array.isArray(tagValuesJson.tagValues)).toBe(true);
    });
  });
});

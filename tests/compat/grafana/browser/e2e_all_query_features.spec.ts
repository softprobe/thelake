import { test, expect } from '@playwright/test';
import { QUERY_FEATURE_CATALOG, QueryFeatureTestCase } from './query_features';

const GRAFANA_URL = process.env.GRAFANA_URL || 'http://127.0.0.1:3000';
const SOFTPROBE_URL = process.env.SOFTPROBE_URL || 'http://127.0.0.1:8090';
const API_KEY = process.env.SOFTPROBE_API_KEY || 'local-dev-key';
const TENANT_ID = process.env.SOFTPROBE_TENANT_ID || 'local-dev-tenant';

const GRAFANA_AUTH_HEADERS = {
  Authorization: 'Basic ' + Buffer.from('admin:admin').toString('base64'),
};

test.describe('Query Features, Functions & Aggregations Verification', () => {
  // Test each query feature in the catalog via Grafana /api/ds/query
  for (const item of QUERY_FEATURE_CATALOG) {
    if (item.category === 'loki') continue; // Tested separately below

    test(`[${item.id}] ${item.category} - ${item.name}: ${item.expr}`, async ({ request }) => {
      const resp = await request.post(`${GRAFANA_URL}/api/ds/query`, {
        headers: {
          ...GRAFANA_AUTH_HEADERS,
          'Content-Type': 'application/json',
        },
        data: {
          queries: [
            {
              refId: 'A',
              datasource: { type: 'prometheus', uid: 'softprobe-prom-a' },
              expr: item.expr,
              range: item.isRange,
              instant: !item.isRange,
            },
          ],
          from: 'now-1h',
          to: 'now',
        },
      });

      expect(resp.status()).toBe(200);
      const json = await resp.json();
      const resultA = json.results?.A;
      expect(resultA).toBeDefined();
      expect(resultA.status ?? 200).toBe(200);

      // Verify frames
      const frames = resultA.frames || [];
      expect(frames.length).toBeGreaterThanOrEqual(1);

      // Also verify direct Prometheus endpoint format via datasource proxy
      const end = Math.floor(Date.now() / 1000);
      const start = end - 3600;
      const proxyResp = await request.post(`${GRAFANA_URL}/api/datasources/proxy/uid/softprobe-prom-a/api/v1/query_range`, {
        headers: {
          ...GRAFANA_AUTH_HEADERS,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: {
          query: item.expr,
          start: start.toString(),
          end: end.toString(),
          step: item.step || '15',
        },
      });

      expect(proxyResp.status()).toBe(200);
      const proxyJson = await proxyResp.json();
      expect(proxyJson.status).toBe('success');
      const seriesList = proxyJson.data?.result || [];

      if (item.validate) {
        const ok = item.validate(seriesList);
        expect(
          ok,
          `validate failed for ${item.id}: series=${seriesList.length} expr=${item.expr}`,
        ).toBe(true);
      }
    });
  }

  // --- Discovery & Protocol API Verification ---
  test.describe('Prometheus Discovery & Protocol APIs', () => {
    test('P-01: GET & POST /api/v1/query (instant query)', async ({ request }) => {
      // GET
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/query`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { query: 'k6_http_reqs' },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data?.resultType).toBe('vector');

      // POST
      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/query`, {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: { query: 'k6_http_reqs' },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
    });

    test('P-02: GET & POST /api/v1/query_range (range query)', async ({ request }) => {
      const end = Math.floor(Date.now() / 1000);
      const start = end - 300;

      // GET
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/query_range`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { query: 'k6_http_reqs', start: start.toString(), end: end.toString(), step: '15' },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data?.resultType).toBe('matrix');

      // POST
      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/query_range`, {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: { query: 'k6_http_reqs', start: start.toString(), end: end.toString(), step: '15' },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
    });

    test('P-03: GET & POST /api/v1/labels discovery', async ({ request }) => {
      // GET
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/labels`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data).toContain('__name__');
      expect(getJson.data).toContain('job');

      // POST
      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/labels`, {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: { 'match[]': 'k6_http_reqs' },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
      expect(postJson.data).toContain('job');
    });

    test('P-04: GET & POST /api/v1/label/{name}/values discovery', async ({ request }) => {
      // GET
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/label/job/values`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { 'match[]': 'k6_http_reqs' },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data).toContain('load-generator');

      // POST
      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/label/job/values`, {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: { 'match[]': 'k6_http_reqs' },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
      expect(postJson.data).toContain('load-generator');
    });

    test('P-05: GET & POST /api/v1/series discovery', async ({ request }) => {
      // GET
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/series`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { 'match[]': 'k6_http_reqs{job="load-generator"}' },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data?.length).toBeGreaterThan(0);

      // POST
      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/series`, {
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'X-Scope-OrgID': TENANT_ID,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        form: { 'match[]': 'k6_http_reqs{job="load-generator"}' },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
      expect(postJson.data?.length).toBeGreaterThan(0);
    });

    test('P-06: GET /api/v1/metadata discovery', async ({ request }) => {
      const resp = await request.get(`${SOFTPROBE_URL}/api/v1/metadata`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { metric: 'k6_http_reqs' },
      });
      expect(resp.status()).toBe(200);
      const json = await resp.json();
      expect(json.status).toBe('success');
      expect(json.data?.k6_http_reqs).toBeDefined();
    });

    test('P-07: GET /api/v1/rules returns empty group array (Grafana compatibility)', async ({ request }) => {
      const resp = await request.get(`${SOFTPROBE_URL}/api/v1/rules`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(resp.status()).toBe(200);
      const json = await resp.json();
      expect(json.status).toBe('success');
      expect(json.data?.groups).toEqual([]);
    });

    test('P-08: GET & POST /api/v1/query_exemplars returns empty array (Grafana compatibility)', async ({ request }) => {
      const getResp = await request.get(`${SOFTPROBE_URL}/api/v1/query_exemplars`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(getResp.status()).toBe(200);
      const getJson = await getResp.json();
      expect(getJson.status).toBe('success');
      expect(getJson.data).toEqual([]);

      const postResp = await request.post(`${SOFTPROBE_URL}/api/v1/query_exemplars`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(postResp.status()).toBe(200);
      const postJson = await postResp.json();
      expect(postJson.status).toBe('success');
      expect(postJson.data).toEqual([]);
    });
  });

  // --- Loki LogQL Features ---
  test.describe('Loki LogQL Features', () => {
    const lokiCases = QUERY_FEATURE_CATALOG.filter(i => i.category === 'loki');
    for (const item of lokiCases) {
      test(`[${item.id}] loki - ${item.name}: ${item.expr}`, async ({ request }) => {
        const endNs = BigInt(Date.now()) * BigInt(1_000_000);
        const startNs = endNs - BigInt(3600) * BigInt(1_000_000_000);

        // Test via Grafana QueryData API (/api/ds/query) with softprobe-loki-a
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

        // Test via Softprobe Loki HTTP API
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

      const valuesResp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/label/service_name/values`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { start: startNs.toString(), end: endNs.toString() },
      });
      expect(valuesResp.status()).toBe(200);
      const valuesJson = await valuesResp.json();
      expect(valuesJson.status).toBe('success');
      expect(valuesJson.data.length).toBeGreaterThan(0);
    });
  });

  // --- Tempo Trace Protocol Endpoints ---
  test.describe('Tempo Protocol Endpoints', () => {
    test('TR-01 & TR-02: Search and trace lookup protocol endpoints', async ({ request }) => {
      // Trace search endpoint returns valid envelope
      const searchResp = await request.get(`${SOFTPROBE_URL}/api/search`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: { limit: '10' },
      });
      expect(searchResp.status()).toBe(200);
      const searchJson = await searchResp.json();
      expect(Array.isArray(searchJson.traces)).toBe(true);

      // Non-existent trace lookup returns protocol-defined 404
      const lookupResp = await request.get(`${SOFTPROBE_URL}/api/traces/00000000000000000000000000000000`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
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

      const tagValuesResp = await request.get(`${SOFTPROBE_URL}/api/search/tag/service.name/values`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
      });
      expect(tagValuesResp.status()).toBe(200);
      const tagValuesJson = await tagValuesResp.json();
      expect(Array.isArray(tagValuesJson.tagValues)).toBe(true);
    });
  });
});

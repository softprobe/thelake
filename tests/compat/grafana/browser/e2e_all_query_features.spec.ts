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
        expect(item.validate(seriesList)).toBe(true);
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
    test('L-01 to L-03: Log stream query with filters and JSON parsing', async ({ request }) => {
      const endNs = BigInt(Date.now()) * BigInt(1_000_000);
      const startNs = endNs - BigInt(3600) * BigInt(1_000_000_000);

      const resp = await request.get(`${SOFTPROBE_URL}/loki/api/v1/query_range`, {
        headers: { Authorization: `Bearer ${API_KEY}`, 'X-Scope-OrgID': TENANT_ID },
        params: {
          query: '{service_name=~".+"} | json',
          start: startNs.toString(),
          end: endNs.toString(),
          limit: '50',
        },
      });

      expect(resp.status()).toBe(200);
      const json = await resp.json();
      expect(json.status).toBe('success');
      expect(json.data?.resultType).toBe('streams');
    });
  });
});

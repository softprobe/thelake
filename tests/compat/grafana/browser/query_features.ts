/**
 * Loki LogQL feature inventory for Grafana browser e2e.
 * Matches docs/compat/query_features_checklist.md (logs only).
 */

export interface QueryFeatureTestCase {
  id: string;
  category: 'loki';
  name: string;
  expr: string;
  isRange: boolean;
  step?: string;
  description: string;
  validate?: (data: any) => boolean;
}

export const QUERY_FEATURE_CATALOG: QueryFeatureTestCase[] = [
  {
    id: 'L-01',
    category: 'loki',
    name: 'loki_stream_selector',
    expr: '{service_name=~".+"}',
    isRange: true,
    description: 'Loki stream selector matching active services',
    validate: (data) => Array.isArray(data) && data.length > 0,
  },
  {
    id: 'L-02',
    category: 'loki',
    name: 'loki_line_filter_contains',
    expr: '{service_name=~".+"} |= "HTTP"',
    isRange: true,
    description: 'Loki line filter containing substring',
    validate: (data) =>
      Array.isArray(data) &&
      data.length > 0 &&
      data.every((s) =>
        (s.values || []).every((pt: [string, string]) => pt[1].includes('HTTP')),
      ),
  },
  {
    id: 'L-03',
    category: 'loki',
    name: 'loki_line_filter_not_contains',
    expr: '{service_name=~".+"} != "DEBUG"',
    isRange: true,
    description: 'Loki line filter excluding substring',
    validate: (data) =>
      Array.isArray(data) &&
      data.length > 0 &&
      data.every((s) =>
        (s.values || []).every((pt: [string, string]) => !pt[1].includes('DEBUG')),
      ),
  },
  {
    id: 'L-04',
    category: 'loki',
    name: 'loki_line_filter_regex',
    expr: '{service_name=~".+"} |~ "GET|POST"',
    isRange: true,
    description: 'Loki line filter matching regular expression',
    validate: (data) =>
      Array.isArray(data) &&
      data.length > 0 &&
      data.every((s) =>
        (s.values || []).every((pt: [string, string]) => /GET|POST/.test(pt[1])),
      ),
  },
  {
    id: 'L-05',
    category: 'loki',
    name: 'loki_json_parser',
    expr: '{service_name=~".+"} | json',
    isRange: true,
    description: 'Loki JSON parser stage extracting attributes',
    validate: (data) => Array.isArray(data),
  },
  {
    id: 'L-06',
    category: 'loki',
    name: 'loki_parsed_field_matcher',
    expr: '{service_name=~".+"} | json | status_code = "200"',
    isRange: true,
    description: 'Loki parsed field equality filter after JSON parser',
    validate: (data) => Array.isArray(data),
  },
];

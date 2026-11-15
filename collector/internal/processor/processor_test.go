package processor_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"

	"github.com/softprobe/thelake/collector/internal/auth"
	"github.com/softprobe/thelake/collector/internal/healthcheck"
	correlationprocessor "github.com/softprobe/thelake/collector/internal/processor"
)

type capturedExport struct {
	headers http.Header
	traces  ptrace.Traces
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func mintAssertion(t *testing.T, secret, wsID, agentID string, caps []string) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := map[string]any{
		"iss":          "softprobe-edge",
		"aud":          "sp-backend",
		"sub":          "e2e-agent",
		"workspace_id": wsID,
		"exp":          time.Now().Unix() + 300,
	}
	if agentID != "" {
		claims["agent_id"] = agentID
	}
	if caps != nil {
		claims["capabilities"] = caps
	}
	raw, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	signing := hdr + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signing + "." + sig
}

func TestEndToEndCollectorPipelineHTTPWireAndCorrelation(t *testing.T) {
	const hmacSecret = "pipeline-e2e-secret"
	const wsID = "550e8400-e29b-41d4-a716-446655440000"

	var mu sync.Mutex
	var exports []capturedExport

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gz.Close()
			reader = gz
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req := ptraceotlp.NewExportRequest()
		if err := req.UnmarshalProto(body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cloned := ptrace.NewTraces()
		req.Traces().CopyTo(cloned)

		mu.Lock()
		exports = append(exports, capturedExport{
			headers: r.Header.Clone(),
			traces:  cloned,
		})
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"partialSuccess":{},"success":true,"ingested_count":1}`))
	}))
	defer upstream.Close()

	otlpPort := freePort(t)
	healthPort := freePort(t)

	cfgYAML := `yaml:
extensions:
  health_check:
    endpoint: 127.0.0.1:` + itoa(healthPort) + `
  softprobe_auth: {}
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:` + itoa(otlpPort) + `
        traces_url_path: /v1/traces
        include_metadata: true
        max_request_body_size: 104857600
processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
    spike_limit_mib: 128
  sessioncorrelation:
    assertion_hmac_secret: ` + hmacSecret + `
    pending_window: 180ms
    resolved_ttl: 1m
    shard_count: 8
exporters:
  otlphttp/thelake:
    traces_endpoint: ` + upstream.URL + `/v1/traces
    encoding: proto
    compression: gzip
    auth:
      authenticator: softprobe_auth
    sending_queue:
      enabled: false
service:
  extensions: [health_check, softprobe_auth]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, sessioncorrelation]
      exporters: [otlphttp/thelake]
`

	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (otelcol.Factories, error) {
			exts, _ := otelcol.MakeFactoryMap[extension.Factory](healthcheck.NewFactory(), auth.NewExtensionFactory())
			recvs, _ := otelcol.MakeFactoryMap[receiver.Factory](otlpreceiver.NewFactory())
			procs, _ := otelcol.MakeFactoryMap[processor.Factory](memorylimiterprocessor.NewFactory(), correlationprocessor.NewFactory())
			exps, _ := otelcol.MakeFactoryMap[exporter.Factory](otlphttpexporter.NewFactory())
			return otelcol.Factories{
				Extensions: exts,
				Receivers:  recvs,
				Processors: procs,
				Exporters:  exps,
			}, nil
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{cfgYAML},
				ProviderFactories: []confmap.ProviderFactory{yamlprovider.NewFactory()},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewCollector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- col.Run(ctx)
	}()

	healthURL := "http://127.0.0.1:" + itoa(healthPort) + "/health"
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	collectorURL := "http://127.0.0.1:" + itoa(otlpPort) + "/v1/traces"
	goodJWT := mintAssertion(t, hmacSecret, wsID, "agent-ota", []string{"ingest"})
	noCapJWT := mintAssertion(t, hmacSecret, wsID, "agent-ota", []string{"query"})

	postProto := func(traces ptrace.Traces, headers map[string]string) int {
		reqProto := ptraceotlp.NewExportRequestFromTraces(traces)
		raw, err := reqProto.MarshalProto()
		if err != nil {
			t.Fatalf("marshal proto: %v", err)
		}
		req, _ := http.NewRequest(http.MethodPost, collectorURL, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/x-protobuf")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/traces: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	var tidCorrelated [16]byte
	tidCorrelated[0] = 0x11
	var sidOBI, sidBrowser [8]byte
	sidOBI[0], sidBrowser[0] = 1, 2

	makeSpan := func(tid [16]byte, sid [8]byte, name string, attrs map[string]string) ptrace.Traces {
		td := ptrace.NewTraces()
		sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID(tid))
		sp.SetSpanID(pcommon.SpanID(sid))
		sp.SetName(name)
		for k, v := range attrs {
			sp.Attributes().PutStr(k, v)
		}
		return td
	}

	if code := postProto(makeSpan(tidCorrelated, sidOBI, "unauth", nil), nil); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", code)
	}
	if code := postProto(makeSpan(tidCorrelated, sidOBI, "forged-jwt", nil), map[string]string{"Authorization": "Bearer a.b.forged"}); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for forged Bearer JWT, got %d", code)
	}
	if code := postProto(makeSpan(tidCorrelated, sidOBI, "forbidden", nil), map[string]string{"X-Softprobe-Assertion": noCapJWT}); code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-ingest agent, got %d", code)
	}
	if code := postProto(makeSpan(tidCorrelated, sidOBI, "capture-id", nil), map[string]string{
		"X-Softprobe-Assertion":  goodJWT,
		"X-Softprobe-Capture-Id": "legacy-cap",
	}); code != http.StatusBadRequest {
		t.Fatalf("expected 400 for X-Softprobe-Capture-Id, got %d", code)
	}

	if code := postProto(makeSpan(tidCorrelated, sidOBI, "GET /api/flights", nil), map[string]string{"X-Softprobe-Assertion": goodJWT}); code != http.StatusOK {
		t.Fatalf("expected 200 on OBI span, got %d", code)
	}
	time.Sleep(40 * time.Millisecond)
	if code := postProto(makeSpan(tidCorrelated, sidBrowser, "fetch /api/flights", map[string]string{"session.id": "sess-live-wire"}), map[string]string{"X-Softprobe-Assertion": goodJWT}); code != http.StatusOK {
		t.Fatalf("expected 200 on Browser span, got %d", code)
	}

	var tidOrphan [16]byte
	tidOrphan[0] = 0x99
	var sidOrphan [8]byte
	sidOrphan[0] = 9
	if code := postProto(makeSpan(tidOrphan, sidOrphan, "GET /orphan", nil), map[string]string{"X-Softprobe-Assertion": goodJWT}); code != http.StatusOK {
		t.Fatalf("expected 200 on assertion orphan span, got %d", code)
	}

	var tidBaggageHdr [16]byte
	tidBaggageHdr[0] = 0x77
	var sidBaggageHdr [8]byte
	sidBaggageHdr[0] = 7
	if code := postProto(makeSpan(tidBaggageHdr, sidBaggageHdr, "POST /baggage-hdr", nil), map[string]string{
		"Authorization": "Bearer smoke-token",
		"Baggage":       "tenant=acme,sp.session.id=sess-http-header",
	}); code != http.StatusOK {
		t.Fatalf("expected 200 on bearer request with Baggage header, got %d", code)
	}

	waitUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(waitUntil) {
		mu.Lock()
		totalSpans := 0
		for _, exp := range exports {
			totalSpans += exp.traces.SpanCount()
		}
		mu.Unlock()
		if totalSpans >= 4 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	col.Shutdown()
	<-doneCh

	mu.Lock()
	defer mu.Unlock()

	foundSessions := map[string]string{}
	for _, exp := range exports {
		if exp.headers.Get("X-Softprobe-Assertion") == "" && exp.headers.Get("Authorization") == "" {
			t.Fatalf("outbound export lacked auth headers: %v", exp.headers)
		}
		if vals, exists := exp.headers["Authorization"]; exists && len(vals) > 0 && vals[0] == "" {
			t.Fatalf("outbound export sent empty Authorization header")
		}
		rs := exp.traces.ResourceSpans()
		for i := 0; i < rs.Len(); i++ {
			ss := rs.At(i).ScopeSpans()
			for j := 0; j < ss.Len(); j++ {
				sps := ss.At(j).Spans()
				for k := 0; k < sps.Len(); k++ {
					sp := sps.At(k)
					sidAttr, ok := sp.Attributes().Get("sp.session.id")
					if ok {
						foundSessions[sp.Name()] = sidAttr.Str()
					} else {
						foundSessions[sp.Name()] = "<empty>"
					}
				}
			}
		}
	}

	if foundSessions["GET /api/flights"] != "sess-live-wire" {
		t.Fatalf("expected OBI span stamped with sess-live-wire, got %q", foundSessions["GET /api/flights"])
	}
	if foundSessions["fetch /api/flights"] != "sess-live-wire" {
		t.Fatalf("expected Browser span stamped with sess-live-wire, got %q", foundSessions["fetch /api/flights"])
	}
	if foundSessions["GET /orphan"] != "<empty>" {
		t.Fatalf("expected orphan span unmodified (<empty>), got %q", foundSessions["GET /orphan"])
	}
	if foundSessions["POST /baggage-hdr"] != "sess-http-header" {
		t.Fatalf("expected HTTP Baggage header session stamped, got %q", foundSessions["POST /baggage-hdr"])
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}

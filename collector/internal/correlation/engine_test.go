package correlation

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/softprobe/thelake/collector/internal/auth"
)

func makeTraceID(b byte) pcommon.TraceID {
	var tid [16]byte
	for i := range tid {
		tid[i] = b
	}
	return pcommon.TraceID(tid)
}

func makeSpanID(b byte) pcommon.SpanID {
	var sid [8]byte
	for i := range sid {
		sid[i] = b
	}
	return pcommon.SpanID(sid)
}

func makeAgentEnv(wsID, agentID, jwt string, expUnix int64) auth.AuthEnvelope {
	return auth.AuthEnvelope{
		Scope: auth.TenantScopeKey{
			Kind:        auth.ScopeAssertion,
			WorkspaceID: wsID,
			AgentID:     agentID,
		},
		Assertion:     jwt,
		ExpiresAtUnix: expUnix,
	}
}

func singleSpanTraces(service string, tid pcommon.TraceID, sid pcommon.SpanID, name string, attrs map[string]string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(tid)
	sp.SetSpanID(sid)
	sp.SetName(name)
	for k, v := range attrs {
		sp.Attributes().PutStr(k, v)
	}
	return td
}

func TestExtractSpanSessionPrecedenceRecordingAndBaggage(t *testing.T) {
	if _, ok := ParseSessionID("sess-valid_123:abc+def"); !ok {
		t.Fatalf("expected valid ASCII session ID")
	}
	for _, bad := range []string{"", "   ", "has space", "ctrl\nchar", "unicode-é", string(make([]byte, 129))} {
		if _, ok := ParseSessionID(bad); ok {
			t.Fatalf("expected invalid session ID for %q", bad)
		}
	}

	t.Run("recording span validation", func(t *testing.T) {
		badRec := singleSpanTraces("web", makeTraceID(1), makeSpanID(1), "rrweb", map[string]string{
			"sp.observation.type": "recording",
		})
		sp := badRec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		if _, isRec, err := ExtractSpanSession(sp, pcommon.NewMap()); !isRec || err == nil {
			t.Fatalf("expected error for recording span without session.id")
		}

		resRec := ptrace.NewTraces()
		resRS := resRec.ResourceSpans().AppendEmpty()
		resRS.Resource().Attributes().PutStr("sp.observation.type", "recording")
		resSpan := resRS.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		if _, isRec, err := ExtractSpanSession(resSpan, resRS.Resource().Attributes()); !isRec || err == nil {
			t.Fatalf("expected error for resource-level recording span without session.id")
		}

		goodRec := singleSpanTraces("web", makeTraceID(1), makeSpanID(1), "rrweb", map[string]string{
			"sp.observation.type": "recording",
			"sp.session.id":       "",
			"session.id":          "sess-rec-1",
		})
		spGood := goodRec.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		sid, isRec, err := ExtractSpanSession(spGood, pcommon.NewMap())
		if err != nil || !isRec || sid != "sess-rec-1" {
			t.Fatalf("unexpected recording result: sid=%q isRec=%v err=%v", sid, isRec, err)
		}

		badAlias := singleSpanTraces("web", makeTraceID(1), makeSpanID(2), "op", map[string]string{
			"session.id":             "bad id with spaces",
			"gen_ai.conversation.id": "bad\nctrl",
		})
		spBadAlias := badAlias.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		if gotSID, _, err := ExtractSpanSession(spBadAlias, pcommon.NewMap()); err != nil || gotSID != "" {
			t.Fatalf("expected empty session ID on invalid aliases, got %q (err=%v)", gotSID, err)
		}
		if _, exists := spBadAlias.Attributes().Get("session.id"); exists {
			t.Fatalf("expected invalid session.id attribute to be stripped")
		}
		if _, exists := spBadAlias.Attributes().Get("gen_ai.conversation.id"); exists {
			t.Fatalf("expected invalid gen_ai.conversation.id attribute to be stripped")
		}
	})

	t.Run("W3C baggage recovery across string, slice, JSON headers, and http.request events", func(t *testing.T) {
		tdSlice := ptrace.NewTraces()
		spSlice := tdSlice.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sl := spSlice.Attributes().PutEmptySlice("http.request.header.baggage")
		sl.AppendEmpty().SetStr("other=1")
		sl.AppendEmpty().SetStr("sp.session.id=sess%3Aslice+ok;ttl=30,session.id=fallback")
		sid, _, err := ExtractSpanSession(spSlice, pcommon.NewMap())
		if err != nil || sid != "sess:slice+ok" {
			t.Fatalf("expected sess:slice+ok from slice baggage, got %q (err=%v)", sid, err)
		}

		tdHdrs := singleSpanTraces("api", makeTraceID(2), makeSpanID(2), "GET /x", map[string]string{
			"http.request.headers": `sp.json:{"Baggage":["foo=bar","session.id=sess-from-json;p=1"]}`,
		})
		spHdrs := tdHdrs.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		sid, _, err = ExtractSpanSession(spHdrs, pcommon.NewMap())
		if err != nil || sid != "sess-from-json" {
			t.Fatalf("expected sess-from-json, got %q (err=%v)", sid, err)
		}

		tdEv := ptrace.NewTraces()
		spEv := tdEv.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		ev := spEv.Events().AppendEmpty()
		ev.SetName("http.request")
		ev.Attributes().PutStr("http.request.header.Baggage", "sp.session.id=sess-from-event")
		sid, _, err = ExtractSpanSession(spEv, pcommon.NewMap())
		if err != nil || sid != "sess-from-event" {
			t.Fatalf("expected sess-from-event, got %q (err=%v)", sid, err)
		}
	})
}

func TestEngineOutOfOrderCorrelationAndUnmodifiedTimeout(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	eng := NewEngine(Config{
		PendingWindow: 2 * time.Second,
		ResolvedTTL:   10 * time.Minute,
		ShardCount:    8,
	})

	ws := "550e8400-e29b-41d4-a716-446655440000"
	obiEnv := makeAgentEnv(ws, "agent-ota", "jwt-obi-t0", now.Unix()+300)
	browserEnv := makeAgentEnv(ws, "agent-ota", "jwt-browser-t1", now.Unix()+301)

	tid := makeTraceID(0x42)

	obiBatch := singleSpanTraces("demo-ota-api", tid, makeSpanID(1), "SELECT flights", nil)
	dec1, err := eng.Ingest(now, obiEnv, obiBatch)
	if err != nil {
		t.Fatalf("obi ingest: %v", err)
	}
	if len(dec1.Immediate) != 0 {
		t.Fatalf("expected OBI unresolved span to hold in pending, got %d immediate batches", len(dec1.Immediate))
	}

	browserBatch := singleSpanTraces("demo-ota-web", tid, makeSpanID(2), "GET /api/flights", map[string]string{
		"session.id": "sess-ota-e2e",
	})
	dec2, err := eng.Ingest(now.Add(50*time.Millisecond), browserEnv, browserBatch)
	if err != nil {
		t.Fatalf("browser ingest: %v", err)
	}
	if len(dec2.Immediate) != 2 {
		t.Fatalf("expected 2 AuthEnvelope-isolated batches (obi + browser), got %d", len(dec2.Immediate))
	}
	for _, b := range dec2.Immediate {
		sp := b.Traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		gotSP, _ := sp.Attributes().Get("sp.session.id")
		gotOTel, _ := sp.Attributes().Get("session.id")
		if gotSP.Str() != "sess-ota-e2e" || gotOTel.Str() != "sess-ota-e2e" {
			t.Fatalf("expected stamped sess-ota-e2e, got sp=%q otel=%q", gotSP.Str(), gotOTel.Str())
		}
	}
	if dec2.Immediate[0].Auth != obiEnv || dec2.Immediate[1].Auth != browserEnv {
		t.Fatalf("expected exact AuthEnvelope preservation per batch")
	}

	lateChild := singleSpanTraces("demo-ota-api", tid, makeSpanID(3), "POST /audit", nil)
	dec3, err := eng.Ingest(now.Add(500*time.Millisecond), obiEnv, lateChild)
	if err != nil || len(dec3.Immediate) != 1 {
		t.Fatalf("expected immediate fast-path export for resolved trace, got %d (err=%v)", len(dec3.Immediate), err)
	}
	lateSpan := dec3.Immediate[0].Traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if got, _ := lateSpan.Attributes().Get("sp.session.id"); got.Str() != "sess-ota-e2e" {
		t.Fatalf("expected late child to inherit sess-ota-e2e, got %q", got.Str())
	}

	for i := 0; i < 32; i++ {
		extra := singleSpanTraces("demo-ota-web", tid, makeSpanID(byte(20+i)), "span", map[string]string{
			"session.id": "sess-ota-e2e",
		})
		if _, err := eng.Ingest(now.Add(600*time.Millisecond), browserEnv, extra); err != nil {
			t.Fatalf("repeat resolved span %d: %v", i, err)
		}
	}
	sh := eng.shardFor(ScopedTraceKey{Scope: browserEnv.Scope, TraceID: tid})
	sh.mu.Lock()
	resolvedFIFOLen := len(sh.resolvedFIFO)
	sh.mu.Unlock()
	if resolvedFIFOLen != 1 {
		t.Fatalf("expected resolvedFIFO to remain bounded to 1 entry per resolved trace, got %d", resolvedFIFOLen)
	}

	orphanTID := makeTraceID(0x99)
	orphanBatch := singleSpanTraces("demo-ota-api", orphanTID, makeSpanID(9), "GET /health", map[string]string{
		"http.request.method": "GET",
	})
	decOrphan, err := eng.Ingest(now, obiEnv, orphanBatch)
	if err != nil || len(decOrphan.Immediate) != 0 {
		t.Fatalf("expected orphan span to hold, got %d", len(decOrphan.Immediate))
	}
	if early := eng.DrainExpired(now.Add(1900 * time.Millisecond)); len(early) != 0 {
		t.Fatalf("must not expire before 2s window")
	}
	expired := eng.DrainExpired(now.Add(2 * time.Second))
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired orphan batch at 2s, got %d", len(expired))
	}
	orphanSpan := expired[0].Traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if _, hasSP := orphanSpan.Attributes().Get("sp.session.id"); hasSP {
		t.Fatalf("expired orphan span must pass through unmodified without sp.session.id")
	}
	if eng.globalPendingSpans.Load() != 0 {
		t.Fatalf("expected 0 pending spans after drain, got %d", eng.globalPendingSpans.Load())
	}
}

func TestEngineTenantIsolationQuotasInvalidBatchAndConcurrency(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	eng := NewEngine(Config{
		PendingWindow:           2 * time.Second,
		ResolvedTTL:             10 * time.Minute,
		ShardCount:              4,
		MaxPendingSpansPerScope: 3,
		MaxPendingSpansGlobal:   100,
	})

	wsA := "550e8400-e29b-41d4-a716-446655440000"
	wsB := "660e8400-e29b-41d4-a716-446655440000"
	envA := makeAgentEnv(wsA, "agent-1", "jwt-a", now.Unix()+300)
	envB := makeAgentEnv(wsB, "agent-1", "jwt-b", now.Unix()+300)

	sharedTID := makeTraceID(0x77)
	_, err := eng.Ingest(now, envA, singleSpanTraces("web", sharedTID, makeSpanID(1), "root", map[string]string{
		"sp.session.id": "sess-tenant-a",
	}))
	if err != nil {
		t.Fatalf("tenant A ingest: %v", err)
	}

	decB, err := eng.Ingest(now, envB, singleSpanTraces("api", sharedTID, makeSpanID(2), "child", nil))
	if err != nil || len(decB.Immediate) != 0 {
		t.Fatalf("tenant B span must hold independently without cross-tenant leak, got %d", len(decB.Immediate))
	}

	scopeCountBefore := eng.scopes.countForTest()
	for i := 0; i < 64; i++ {
		var h [32]byte
		h[0] = byte(i + 1)
		opaqueEnv := auth.AuthEnvelope{
			Scope: auth.TenantScopeKey{
				Kind:           auth.ScopeBearerHash,
				CredentialHash: h,
			},
			Authorization: fmt.Sprintf("Bearer random-unverified-%d", i),
		}
		decOpaque, err := eng.IngestWithBaggage(
			now,
			opaqueEnv,
			"sess-req-baggage",
			singleSpanTraces("cli", makeTraceID(byte(0xd0+i)), makeSpanID(1), "run", nil),
		)
		if err != nil || len(decOpaque.Immediate) != 1 {
			t.Fatalf("opaque bearer must forward on 0ms stateless fast path, got %d (err=%v)", len(decOpaque.Immediate), err)
		}
		stamped, _ := decOpaque.Immediate[0].Traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("sp.session.id")
		if stamped.Str() != "sess-req-baggage" {
			t.Fatalf("expected request baggage stamped on fast path, got %q", stamped.Str())
		}
	}
	if got := eng.scopes.countForTest(); got != scopeCountBefore {
		t.Fatalf("unverified opaque bearer tokens must allocate 0 scopeCounter entries, before=%d after=%d", scopeCountBefore, got)
	}
	if gotB := eng.countersFor(envB.Scope).pendingSpans.Load(); gotB != 1 {
		t.Fatalf("opaque bearer traffic must not evict tenant B pending span, got %d", gotB)
	}

	badRec := singleSpanTraces("web", makeTraceID(0x88), makeSpanID(3), "rrweb", map[string]string{
		"sp.observation.type": "recording",
		"sp.session.id":       "bad session id",
	})
	if _, err := eng.Ingest(now, envA, badRec); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for invalid recording, got %v", err)
	}

	evictedTotal := 0
	for i := 0; i < 5; i++ {
		dec, err := eng.Ingest(
			now.Add(time.Duration(i)*time.Millisecond),
			envA,
			singleSpanTraces("noisy", makeTraceID(byte(0xa0+i)), makeSpanID(byte(10+i)), "GET /ping", nil),
		)
		if err != nil {
			t.Fatalf("flood %d: %v", i, err)
		}
		for _, b := range dec.Immediate {
			if b.Auth != envA {
				t.Fatalf("noisy eviction leaked another tenant's batch")
			}
			evictedTotal += b.Traces.SpanCount()
		}
	}
	if evictedTotal != 2 {
		t.Fatalf("expected 2 overflow spans evicted unmodified for noisy scope, got %d", evictedTotal)
	}
	if gotB := eng.countersFor(envB.Scope).pendingSpans.Load(); gotB != 1 {
		t.Fatalf("expected quiet tenant B pending span untouched (1), got %d", gotB)
	}

	expiringEnv := makeAgentEnv(wsA, "agent-2", "jwt-expiring", now.Unix()+1)
	decExp, err := eng.Ingest(now, expiringEnv, singleSpanTraces("api", makeTraceID(0xfe), makeSpanID(99), "GET /x", nil))
	if err != nil || len(decExp.Immediate) != 1 {
		t.Fatalf("expected near-expiry JWT span to bypass hold immediately, got %d", len(decExp.Immediate))
	}

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tid := makeTraceID(byte(0xc0 + (id % 4)))
			attrs := map[string]string{}
			if id%2 == 0 {
				attrs["sp.session.id"] = fmt.Sprintf("sess-race-%d", id%4)
			}
			_, _ = eng.Ingest(now.Add(time.Duration(id)*time.Millisecond), envA, singleSpanTraces("svc", tid, makeSpanID(byte(id+1)), "op", attrs))
			_ = eng.DrainExpired(now.Add(3 * time.Second))
		}(g)
	}
	wg.Wait()
	_ = eng.DrainAll()
}

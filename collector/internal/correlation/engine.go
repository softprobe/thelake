package correlation

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/softprobe/thelake/collector/internal/auth"
)

type ScopedTraceKey struct {
	Scope   auth.TenantScopeKey
	TraceID pcommon.TraceID
}

type Config struct {
	PendingWindow             time.Duration
	ResolvedTTL               time.Duration
	ShardCount                int
	MaxResolvedTracesPerScope int
	MaxResolvedTracesGlobal   int
	MaxPendingSpansPerScope   int
	MaxPendingSpansGlobal     int
	MaxPendingSpansPerTrace   int
}

func (c Config) withDefaults() Config {
	if c.PendingWindow == 0 {
		c.PendingWindow = 2 * time.Second
	}
	if c.ResolvedTTL <= 0 {
		c.ResolvedTTL = 10 * time.Minute
	}
	if c.ShardCount <= 0 {
		c.ShardCount = 64
	} else {
		p := 1
		for p < c.ShardCount {
			p <<= 1
		}
		c.ShardCount = p
	}
	if c.MaxResolvedTracesPerScope <= 0 {
		c.MaxResolvedTracesPerScope = 4096
	}
	if c.MaxResolvedTracesGlobal <= 0 {
		c.MaxResolvedTracesGlobal = 65536
	}
	if c.MaxPendingSpansPerScope <= 0 {
		c.MaxPendingSpansPerScope = 4096
	}
	if c.MaxPendingSpansGlobal <= 0 {
		c.MaxPendingSpansGlobal = 32768
	}
	if c.MaxPendingSpansPerTrace <= 0 {
		c.MaxPendingSpansPerTrace = 256
	}
	return c
}

type OutboundBatch struct {
	Auth   auth.AuthEnvelope
	Traces ptrace.Traces
}

type IngestDecision struct {
	Immediate []OutboundBatch
}

type traceStage uint8

const (
	stagePending traceStage = iota + 1
	stageResolved
)

type heldBatch struct {
	auth      auth.AuthEnvelope
	traces    ptrace.Traces
	spanCount int
}

type traceEntry struct {
	stage     traceStage
	seq       uint64
	session   SessionID
	deadline  time.Time
	held      []heldBatch
	heldSpans int
}

type pendingQueueItem struct {
	key      ScopedTraceKey
	seq      uint64
	deadline time.Time
}

type resolvedQueueItem struct {
	key      ScopedTraceKey
	seq      uint64
	deadline time.Time
}

type shard struct {
	mu           sync.Mutex
	seq          uint64
	entries      map[ScopedTraceKey]*traceEntry
	pendingFIFO  []pendingQueueItem
	resolvedFIFO []resolvedQueueItem
}

type scopeCounters struct {
	resolvedTraces atomic.Int64
	pendingSpans   atomic.Int64
}

type scopeCounterMap struct {
	entries sync.Map
}

func (m *scopeCounterMap) getOrCreate(scope auth.TenantScopeKey) *scopeCounters {
	if existing, ok := m.entries.Load(scope); ok {
		return existing.(*scopeCounters)
	}
	fresh := &scopeCounters{}
	actual, _ := m.entries.LoadOrStore(scope, fresh)
	return actual.(*scopeCounters)
}

func (m *scopeCounterMap) countForTest() int {
	n := 0
	m.entries.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

type Engine struct {
	cfg                Config
	shards             []*shard
	mask               uint64
	globalResolved     atomic.Int64
	globalPendingSpans atomic.Int64
	scopes             scopeCounterMap
}

func NewEngine(cfg Config) *Engine {
	cfg = cfg.withDefaults()
	shards := make([]*shard, cfg.ShardCount)
	for i := range shards {
		shards[i] = &shard{
			entries: make(map[ScopedTraceKey]*traceEntry),
		}
	}
	return &Engine{
		cfg:    cfg,
		shards: shards,
		mask:   uint64(cfg.ShardCount - 1),
	}
}

func (e *Engine) countersFor(scope auth.TenantScopeKey) *scopeCounters {
	return e.scopes.getOrCreate(scope)
}

func (e *Engine) shardFor(key ScopedTraceKey) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(key.Scope.Kind)})
	_, _ = h.Write([]byte(key.Scope.WorkspaceID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key.Scope.AgentID))
	_, _ = h.Write(key.Scope.CredentialHash[:])
	_, _ = h.Write(key.TraceID[:])
	return e.shards[h.Sum64()&e.mask]
}

type spanRef struct {
	rIdx        int
	sIdx        int
	spIdx       int
	isRecording bool
	sid         SessionID
}

func (e *Engine) Ingest(now time.Time, env auth.AuthEnvelope, td ptrace.Traces) (IngestDecision, error) {
	return e.IngestWithBaggage(now, env, "", td)
}

func (e *Engine) IngestWithBaggage(now time.Time, env auth.AuthEnvelope, requestBaggage SessionID, td ptrace.Traces) (IngestDecision, error) {
	rSpans := td.ResourceSpans()
	var refs []spanRef

	for rIdx := 0; rIdx < rSpans.Len(); rIdx++ {
		rSpan := rSpans.At(rIdx)
		resAttrs := rSpan.Resource().Attributes()
		sSpans := rSpan.ScopeSpans()
		for sIdx := 0; sIdx < sSpans.Len(); sIdx++ {
			spans := sSpans.At(sIdx).Spans()
			for spIdx := 0; spIdx < spans.Len(); spIdx++ {
				sid, isRec, err := ExtractSpanSession(spans.At(spIdx), resAttrs)
				if err != nil {
					return IngestDecision{}, status.Error(codes.InvalidArgument, err.Error())
				}
				if !isRec && sid == "" && requestBaggage != "" {
					sid = requestBaggage
				}
				refs = append(refs, spanRef{
					rIdx:        rIdx,
					sIdx:        sIdx,
					spIdx:       spIdx,
					isRecording: isRec,
					sid:         sid,
				})
			}
		}
	}

	if len(refs) == 0 {
		return IngestDecision{}, nil
	}

	if env.Scope.Kind != auth.ScopeAssertion {
		for _, ref := range refs {
			if ref.isRecording || ref.sid == "" {
				continue
			}
			sp := rSpans.At(ref.rIdx).ScopeSpans().At(ref.sIdx).Spans().At(ref.spIdx)
			StampSpanSession(sp, ref.sid)
		}
		out := ptrace.NewTraces()
		td.CopyTo(out)
		return IngestDecision{
			Immediate: []OutboundBatch{{
				Auth:   env,
				Traces: out,
			}},
		}, nil
	}

	var rawOutbound []OutboundBatch
	resolvedTouch := false

	for i := range refs {
		ref := &refs[i]
		if ref.isRecording || ref.sid == "" {
			continue
		}
		sp := rSpans.At(ref.rIdx).ScopeSpans().At(ref.sIdx).Spans().At(ref.spIdx)
		tid := sp.TraceID()
		if tid.IsEmpty() {
			StampSpanSession(sp, ref.sid)
			continue
		}
		canonicalSID, released := e.resolveTrace(now, ScopedTraceKey{Scope: env.Scope, TraceID: tid}, ref.sid)
		ref.sid = canonicalSID
		StampSpanSession(sp, canonicalSID)
		rawOutbound = append(rawOutbound, released...)
		resolvedTouch = true
	}

	if resolvedTouch {
		e.enforceResolvedQuotas(now, env.Scope)
	}

	readyIndices := make([]bool, len(refs))
	hasReady := false
	unresolvedByTrace := make(map[pcommon.TraceID][]int)
	var traceOrder []pcommon.TraceID

	for i, ref := range refs {
		sp := rSpans.At(ref.rIdx).ScopeSpans().At(ref.sIdx).Spans().At(ref.spIdx)
		tid := sp.TraceID()
		if ref.isRecording || ref.sid != "" || tid.IsEmpty() {
			readyIndices[i] = true
			hasReady = true
			continue
		}
		if _, exists := unresolvedByTrace[tid]; !exists {
			traceOrder = append(traceOrder, tid)
		}
		unresolvedByTrace[tid] = append(unresolvedByTrace[tid], i)
	}

	pendingAdded := false
	for _, tid := range traceOrder {
		idxs := unresolvedByTrace[tid]
		key := ScopedTraceKey{Scope: env.Scope, TraceID: tid}
		sh := e.shardFor(key)
		sc := e.countersFor(env.Scope)

		sh.mu.Lock()
		if entry, ok := sh.entries[key]; ok && entry.stage == stageResolved {
			if !now.Before(entry.deadline) {
				delete(sh.entries, key)
				sc.resolvedTraces.Add(-1)
				e.globalResolved.Add(-1)
			} else {
				sid := entry.session
				sh.mu.Unlock()
				for _, refIdx := range idxs {
					ref := refs[refIdx]
					sp := rSpans.At(ref.rIdx).ScopeSpans().At(ref.sIdx).Spans().At(ref.spIdx)
					StampSpanSession(sp, sid)
					readyIndices[refIdx] = true
				}
				hasReady = true
				continue
			}
		}

		deadline, canHold := e.computeHoldDeadline(now, env)
		groupCount := len(idxs)
		if !canHold || groupCount > e.cfg.MaxPendingSpansPerTrace {
			if entry, ok := sh.entries[key]; ok && entry.stage == stagePending {
				rawOutbound = append(rawOutbound, e.popPendingLocked(sh, key, entry)...)
			}
			sh.mu.Unlock()
			for _, refIdx := range idxs {
				readyIndices[refIdx] = true
			}
			hasReady = true
			continue
		}

		if entry, ok := sh.entries[key]; ok && entry.stage == stagePending {
			if entry.heldSpans+groupCount > e.cfg.MaxPendingSpansPerTrace {
				rawOutbound = append(rawOutbound, e.popPendingLocked(sh, key, entry)...)
				sh.mu.Unlock()
				for _, refIdx := range idxs {
					readyIndices[refIdx] = true
				}
				hasReady = true
				continue
			}
			copied := cloneSelectedSpans(td, refs, idxs)
			entry.held = append(entry.held, heldBatch{
				auth:      env,
				traces:    copied,
				spanCount: groupCount,
			})
			entry.heldSpans += groupCount
			sc.pendingSpans.Add(int64(groupCount))
			e.globalPendingSpans.Add(int64(groupCount))
			sh.mu.Unlock()
			pendingAdded = true
			continue
		}

		copied := cloneSelectedSpans(td, refs, idxs)
		sh.seq++
		seq := sh.seq
		sh.entries[key] = &traceEntry{
			stage:    stagePending,
			seq:      seq,
			deadline: deadline,
			held: []heldBatch{{
				auth:      env,
				traces:    copied,
				spanCount: groupCount,
			}},
			heldSpans: groupCount,
		}
		sh.pendingFIFO = append(sh.pendingFIFO, pendingQueueItem{
			key:      key,
			seq:      seq,
			deadline: deadline,
		})
		sc.pendingSpans.Add(int64(groupCount))
		e.globalPendingSpans.Add(int64(groupCount))
		sh.mu.Unlock()
		pendingAdded = true
	}

	if pendingAdded {
		rawOutbound = append(rawOutbound, e.enforcePendingQuotas(env.Scope)...)
	}

	if hasReady {
		var selected []int
		for i, ok := range readyIndices {
			if ok {
				selected = append(selected, i)
			}
		}
		readyTraces := cloneSelectedSpans(td, refs, selected)
		rawOutbound = append(rawOutbound, OutboundBatch{
			Auth:   env,
			Traces: readyTraces,
		})
	}

	return IngestDecision{
		Immediate: mergeOutboundBatches(rawOutbound),
	}, nil
}

func (e *Engine) computeHoldDeadline(now time.Time, env auth.AuthEnvelope) (time.Time, bool) {
	if e.cfg.PendingWindow <= 0 {
		return time.Time{}, false
	}
	deadline := now.Add(e.cfg.PendingWindow)
	if env.ExpiresAtUnix > 0 {
		safeExp := time.Unix(env.ExpiresAtUnix, 0).Add(-1 * time.Second)
		if !safeExp.After(now) {
			return time.Time{}, false
		}
		if safeExp.Before(deadline) {
			deadline = safeExp
		}
	}
	return deadline, true
}

func (e *Engine) resolveTrace(now time.Time, key ScopedTraceKey, incoming SessionID) (SessionID, []OutboundBatch) {
	sh := e.shardFor(key)
	sc := e.countersFor(key.Scope)
	exp := now.Add(e.cfg.ResolvedTTL)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if entry, ok := sh.entries[key]; ok {
		if entry.stage == stageResolved && now.Before(entry.deadline) {
			return entry.session, nil
		}
		if entry.stage == stageResolved {
			delete(sh.entries, key)
			sc.resolvedTraces.Add(-1)
			e.globalResolved.Add(-1)
		} else if entry.stage == stagePending {
			held := entry.held
			heldCount := entry.heldSpans
			sc.pendingSpans.Add(-int64(heldCount))
			e.globalPendingSpans.Add(-int64(heldCount))

			sh.seq++
			entry.stage = stageResolved
			entry.seq = sh.seq
			entry.session = incoming
			entry.deadline = exp
			entry.held = nil
			entry.heldSpans = 0
			sh.resolvedFIFO = append(sh.resolvedFIFO, resolvedQueueItem{
				key:      key,
				seq:      entry.seq,
				deadline: exp,
			})
			sc.resolvedTraces.Add(1)
			e.globalResolved.Add(1)

			out := make([]OutboundBatch, 0, len(held))
			for _, hb := range held {
				stampAllSpans(hb.traces, incoming)
				out = append(out, OutboundBatch{
					Auth:   hb.auth,
					Traces: hb.traces,
				})
			}
			return incoming, out
		}
	}

	sh.seq++
	seq := sh.seq
	sh.entries[key] = &traceEntry{
		stage:    stageResolved,
		seq:      seq,
		session:  incoming,
		deadline: exp,
	}
	sh.resolvedFIFO = append(sh.resolvedFIFO, resolvedQueueItem{
		key:      key,
		seq:      seq,
		deadline: exp,
	})
	sc.resolvedTraces.Add(1)
	e.globalResolved.Add(1)
	return incoming, nil
}

func (e *Engine) popPendingLocked(sh *shard, key ScopedTraceKey, entry *traceEntry) []OutboundBatch {
	held := entry.held
	heldCount := entry.heldSpans
	delete(sh.entries, key)
	sc := e.countersFor(key.Scope)
	sc.pendingSpans.Add(-int64(heldCount))
	e.globalPendingSpans.Add(-int64(heldCount))

	out := make([]OutboundBatch, 0, len(held))
	for _, hb := range held {
		out = append(out, OutboundBatch{
			Auth:   hb.auth,
			Traces: hb.traces,
		})
	}
	return out
}

func (e *Engine) enforcePendingQuotas(scope auth.TenantScopeKey) []OutboundBatch {
	sc := e.countersFor(scope)
	var evicted []OutboundBatch

	for sc.pendingSpans.Load() > int64(e.cfg.MaxPendingSpansPerScope) {
		batch, ok := e.evictOldestPending(true, scope)
		if !ok {
			break
		}
		evicted = append(evicted, batch...)
	}
	for e.globalPendingSpans.Load() > int64(e.cfg.MaxPendingSpansGlobal) {
		batch, ok := e.evictOldestPending(false, auth.TenantScopeKey{})
		if !ok {
			break
		}
		evicted = append(evicted, batch...)
	}
	return evicted
}

func (e *Engine) evictOldestPending(filterScope bool, scope auth.TenantScopeKey) ([]OutboundBatch, bool) {
	bestShard := -1
	var bestItem pendingQueueItem

	for i, sh := range e.shards {
		sh.mu.Lock()
		sh.compactPendingHeadLocked()
		for _, item := range sh.pendingFIFO {
			entry, ok := sh.entries[item.key]
			if !ok || entry.stage != stagePending || entry.seq != item.seq {
				continue
			}
			if filterScope && item.key.Scope != scope {
				continue
			}
			if bestShard == -1 || item.deadline.Before(bestItem.deadline) {
				bestShard = i
				bestItem = item
			}
			break
		}
		sh.mu.Unlock()
	}

	if bestShard == -1 {
		return nil, false
	}

	sh := e.shards[bestShard]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	entry, ok := sh.entries[bestItem.key]
	if !ok || entry.stage != stagePending || entry.seq != bestItem.seq {
		return nil, true
	}
	out := e.popPendingLocked(sh, bestItem.key, entry)
	sh.compactPendingHeadLocked()
	return out, true
}

func (e *Engine) enforceResolvedQuotas(now time.Time, scope auth.TenantScopeKey) {
	sc := e.countersFor(scope)
	if sc.resolvedTraces.Load() <= int64(e.cfg.MaxResolvedTracesPerScope) &&
		e.globalResolved.Load() <= int64(e.cfg.MaxResolvedTracesGlobal) {
		return
	}

	for _, sh := range e.shards {
		sh.mu.Lock()
		sh.sweepExpiredResolvedLocked(now, e)
		sh.mu.Unlock()
	}

	for sc.resolvedTraces.Load() > int64(e.cfg.MaxResolvedTracesPerScope) {
		if !e.evictOldestResolved(true, scope) {
			break
		}
	}
	for e.globalResolved.Load() > int64(e.cfg.MaxResolvedTracesGlobal) {
		if !e.evictOldestResolved(false, auth.TenantScopeKey{}) {
			break
		}
	}
}

func (e *Engine) evictOldestResolved(filterScope bool, scope auth.TenantScopeKey) bool {
	bestShard := -1
	var bestItem resolvedQueueItem

	for i, sh := range e.shards {
		sh.mu.Lock()
		sh.compactResolvedHeadLocked()
		for _, item := range sh.resolvedFIFO {
			entry, ok := sh.entries[item.key]
			if !ok || entry.stage != stageResolved || entry.seq != item.seq {
				continue
			}
			if filterScope && item.key.Scope != scope {
				continue
			}
			if bestShard == -1 || item.deadline.Before(bestItem.deadline) {
				bestShard = i
				bestItem = item
			}
			break
		}
		sh.mu.Unlock()
	}

	if bestShard == -1 {
		return false
	}
	sh := e.shards[bestShard]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	entry, ok := sh.entries[bestItem.key]
	if ok && entry.stage == stageResolved && entry.seq == bestItem.seq {
		delete(sh.entries, bestItem.key)
		e.countersFor(bestItem.key.Scope).resolvedTraces.Add(-1)
		e.globalResolved.Add(-1)
	}
	sh.compactResolvedHeadLocked()
	return true
}

func (e *Engine) DrainExpired(now time.Time) []OutboundBatch {
	var raw []OutboundBatch
	for _, sh := range e.shards {
		sh.mu.Lock()
		sh.sweepExpiredResolvedLocked(now, e)
		for len(sh.pendingFIFO) > 0 {
			head := sh.pendingFIFO[0]
			entry, ok := sh.entries[head.key]
			if !ok || entry.stage != stagePending || entry.seq != head.seq {
				sh.pendingFIFO = sh.pendingFIFO[1:]
				continue
			}
			if now.Before(entry.deadline) {
				break
			}
			sh.pendingFIFO = sh.pendingFIFO[1:]
			raw = append(raw, e.popPendingLocked(sh, head.key, entry)...)
		}
		if len(sh.pendingFIFO) == 0 {
			sh.pendingFIFO = nil
		}
		sh.mu.Unlock()
	}
	return mergeOutboundBatches(raw)
}

func (e *Engine) DrainAll() []OutboundBatch {
	var raw []OutboundBatch
	for _, sh := range e.shards {
		sh.mu.Lock()
		for _, item := range sh.pendingFIFO {
			entry, ok := sh.entries[item.key]
			if !ok || entry.stage != stagePending || entry.seq != item.seq {
				continue
			}
			raw = append(raw, e.popPendingLocked(sh, item.key, entry)...)
		}
		sh.pendingFIFO = nil
		sh.mu.Unlock()
	}
	return mergeOutboundBatches(raw)
}

func (sh *shard) compactPendingHeadLocked() {
	for len(sh.pendingFIFO) > 0 {
		head := sh.pendingFIFO[0]
		entry, ok := sh.entries[head.key]
		if ok && entry.stage == stagePending && entry.seq == head.seq {
			return
		}
		sh.pendingFIFO = sh.pendingFIFO[1:]
	}
	sh.pendingFIFO = nil
}

func (sh *shard) compactResolvedHeadLocked() {
	for len(sh.resolvedFIFO) > 0 {
		head := sh.resolvedFIFO[0]
		entry, ok := sh.entries[head.key]
		if ok && entry.stage == stageResolved && entry.seq == head.seq {
			return
		}
		sh.resolvedFIFO = sh.resolvedFIFO[1:]
	}
	sh.resolvedFIFO = nil
}

func (sh *shard) sweepExpiredResolvedLocked(now time.Time, e *Engine) {
	for len(sh.resolvedFIFO) > 0 {
		head := sh.resolvedFIFO[0]
		entry, ok := sh.entries[head.key]
		if !ok || entry.stage != stageResolved || entry.seq != head.seq {
			sh.resolvedFIFO = sh.resolvedFIFO[1:]
			continue
		}
		if now.Before(entry.deadline) {
			return
		}
		sh.resolvedFIFO = sh.resolvedFIFO[1:]
		delete(sh.entries, head.key)
		e.countersFor(head.key.Scope).resolvedTraces.Add(-1)
		e.globalResolved.Add(-1)
	}
	sh.resolvedFIFO = nil
}

func stampAllSpans(td ptrace.Traces, sid SessionID) {
	rSpans := td.ResourceSpans()
	for i := 0; i < rSpans.Len(); i++ {
		sSpans := rSpans.At(i).ScopeSpans()
		for j := 0; j < sSpans.Len(); j++ {
			spans := sSpans.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				StampSpanSession(spans.At(k), sid)
			}
		}
	}
}

func cloneSelectedSpans(src ptrace.Traces, refs []spanRef, selected []int) ptrace.Traces {
	dst := ptrace.NewTraces()
	srcR := src.ResourceSpans()

	type rsPair struct{ rIdx, sIdx int }
	rMap := make(map[int]ptrace.ResourceSpans)
	sMap := make(map[rsPair]ptrace.ScopeSpans)

	for _, idx := range selected {
		ref := refs[idx]
		srcRS := srcR.At(ref.rIdx)
		dstRS, ok := rMap[ref.rIdx]
		if !ok {
			dstRS = dst.ResourceSpans().AppendEmpty()
			srcRS.Resource().CopyTo(dstRS.Resource())
			dstRS.SetSchemaUrl(srcRS.SchemaUrl())
			rMap[ref.rIdx] = dstRS
		}
		pair := rsPair{rIdx: ref.rIdx, sIdx: ref.sIdx}
		dstSS, ok := sMap[pair]
		if !ok {
			srcSS := srcRS.ScopeSpans().At(ref.sIdx)
			dstSS = dstRS.ScopeSpans().AppendEmpty()
			srcSS.Scope().CopyTo(dstSS.Scope())
			dstSS.SetSchemaUrl(srcSS.SchemaUrl())
			sMap[pair] = dstSS
		}
		srcSpan := srcRS.ScopeSpans().At(ref.sIdx).Spans().At(ref.spIdx)
		srcSpan.CopyTo(dstSS.Spans().AppendEmpty())
	}
	return dst
}

func mergeOutboundBatches(batches []OutboundBatch) []OutboundBatch {
	if len(batches) <= 1 {
		return batches
	}
	var merged []OutboundBatch
	indexByAuth := make(map[auth.AuthEnvelope]int, len(batches))
	for _, b := range batches {
		if b.Traces.SpanCount() == 0 {
			continue
		}
		if idx, exists := indexByAuth[b.Auth]; exists {
			srcRS := b.Traces.ResourceSpans()
			dstRS := merged[idx].Traces.ResourceSpans()
			for i := 0; i < srcRS.Len(); i++ {
				srcRS.At(i).CopyTo(dstRS.AppendEmpty())
			}
			continue
		}
		dst := ptrace.NewTraces()
		b.Traces.CopyTo(dst)
		indexByAuth[b.Auth] = len(merged)
		merged = append(merged, OutboundBatch{
			Auth:   b.Auth,
			Traces: dst,
		})
	}
	return merged
}

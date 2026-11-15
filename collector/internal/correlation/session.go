package correlation

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var ErrInvalidRecordingSession = errors.New("recording requires a valid span-level session.id")

type SessionID string

var sessionIDPrecedence = [...]string{
	"sp.session.id",
	"sp_session_id",
	"session.id",
	"gen_ai.conversation.id",
	"traceloop.association.properties.session_id",
	"ai.telemetry.metadata.sessionId",
	"enduser.session.id",
}

var recordingSessionKeys = [...]string{
	"sp.session.id",
	"sp_session_id",
	"session.id",
}

func ParseSessionID(raw string) (SessionID, bool) {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) == 0 || len(trimmed) > 128 {
		return "", false
	}
	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if b < 0x21 || b > 0x7E {
			return "", false
		}
	}
	return SessionID(trimmed), true
}

func StampSpanSession(span ptrace.Span, sid SessionID) {
	if sid == "" {
		return
	}
	attrs := span.Attributes()
	attrs.PutStr("sp.session.id", string(sid))
	if existing, ok := attrs.Get("session.id"); !ok || strings.TrimSpace(existing.AsString()) == "" {
		attrs.PutStr("session.id", string(sid))
	}
}

func ExtractSpanSession(span ptrace.Span, resourceAttrs pcommon.Map) (SessionID, bool, error) {
	attrs := span.Attributes()
	if isRecordingSpan(attrs, resourceAttrs) {
		for _, key := range recordingSessionKeys {
			if val, ok := attrs.Get(key); ok && val.Type() == pcommon.ValueTypeStr {
				if sid, valid := ParseSessionID(val.Str()); valid {
					stripInvalidSessionAliases(attrs)
					stripInvalidSessionAliases(resourceAttrs)
					attrs.PutStr("sp.session.id", string(sid))
					return sid, true, nil
				}
			}
		}
		return "", true, ErrInvalidRecordingSession
	}

	var resolved SessionID
	for _, key := range sessionIDPrecedence {
		if val, ok := attrs.Get(key); ok && val.Type() == pcommon.ValueTypeStr {
			if sid, valid := ParseSessionID(val.Str()); valid {
				resolved = sid
				break
			}
		}
	}
	if resolved == "" {
		for _, key := range sessionIDPrecedence {
			if val, ok := resourceAttrs.Get(key); ok && val.Type() == pcommon.ValueTypeStr {
				if sid, valid := ParseSessionID(val.Str()); valid {
					resolved = sid
					break
				}
			}
		}
	}

	stripInvalidSessionAliases(attrs)
	stripInvalidSessionAliases(resourceAttrs)
	if resolved != "" {
		return resolved, false, nil
	}

	if sid, ok := extractBaggageFromMap(attrs); ok {
		return sid, false, nil
	}
	events := span.Events()
	for i := 0; i < events.Len(); i++ {
		ev := events.At(i)
		if ev.Name() == "http.request" {
			if sid, ok := extractBaggageFromMap(ev.Attributes()); ok {
				return sid, false, nil
			}
		}
	}

	return "", false, nil
}

func isRecordingSpan(attrs, resourceAttrs pcommon.Map) bool {
	if val, ok := attrs.Get("sp.observation.type"); ok && val.Type() == pcommon.ValueTypeStr && strings.TrimSpace(val.Str()) == "recording" {
		return true
	}
	if val, ok := resourceAttrs.Get("sp.observation.type"); ok && val.Type() == pcommon.ValueTypeStr && strings.TrimSpace(val.Str()) == "recording" {
		return true
	}
	return false
}

func stripInvalidSessionAliases(attrs pcommon.Map) {
	for _, key := range sessionIDPrecedence {
		val, ok := attrs.Get(key)
		if !ok {
			continue
		}
		if val.Type() != pcommon.ValueTypeStr {
			attrs.Remove(key)
			continue
		}
		if _, valid := ParseSessionID(val.Str()); !valid {
			attrs.Remove(key)
		}
	}
}

func ExtractMetadataBaggage(values []string) (SessionID, bool) {
	var fallback SessionID
	for _, header := range values {
		sid, ok := parseBaggageHeader(header)
		if !ok {
			continue
		}
		if strings.Contains(header, "sp.session.id") {
			return sid, true
		}
		if fallback == "" {
			fallback = sid
		}
	}
	if fallback != "" {
		return fallback, true
	}
	return "", false
}

func extractBaggageFromMap(attrs pcommon.Map) (SessionID, bool) {
	for _, key := range [...]string{"http.request.header.baggage", "http.request.header.Baggage"} {
		if val, ok := attrs.Get(key); ok {
			if sid, found := extractBaggageFromValue(val); found {
				return sid, true
			}
		}
	}
	if hdrsVal, ok := attrs.Get("http.request.headers"); ok && hdrsVal.Type() == pcommon.ValueTypeStr {
		raw := stripNestedJSONPrefix(hdrsVal.Str())
		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(raw), &obj) == nil {
			for k, v := range obj {
				if !strings.EqualFold(k, "baggage") {
					continue
				}
				var single string
				if json.Unmarshal(v, &single) == nil {
					if sid, found := parseBaggageHeader(single); found {
						return sid, true
					}
				}
				var multi []string
				if json.Unmarshal(v, &multi) == nil {
					for _, item := range multi {
						if sid, found := parseBaggageHeader(item); found {
							return sid, true
						}
					}
				}
			}
		}
	}
	return "", false
}

func extractBaggageFromValue(val pcommon.Value) (SessionID, bool) {
	switch val.Type() {
	case pcommon.ValueTypeStr:
		s := stripNestedJSONPrefix(val.Str())
		if strings.HasPrefix(strings.TrimSpace(s), "[") {
			var arr []string
			if json.Unmarshal([]byte(s), &arr) == nil {
				for _, item := range arr {
					if sid, ok := parseBaggageHeader(item); ok {
						return sid, true
					}
				}
			}
		}
		return parseBaggageHeader(s)
	case pcommon.ValueTypeSlice:
		slice := val.Slice()
		for i := 0; i < slice.Len(); i++ {
			elem := slice.At(i)
			if elem.Type() == pcommon.ValueTypeStr {
				if sid, ok := parseBaggageHeader(elem.Str()); ok {
					return sid, true
				}
			}
		}
	}
	return "", false
}

func parseBaggageHeader(header string) (SessionID, bool) {
	var fallback SessionID
	for _, member := range strings.Split(header, ",") {
		kvPart, _, _ := strings.Cut(member, ";")
		k, v, hasEq := strings.Cut(kvPart, "=")
		if !hasEq {
			continue
		}
		key := strings.TrimSpace(k)
		if key != "sp.session.id" && key != "session.id" {
			continue
		}
		rawVal := strings.TrimSpace(v)
		decoded, err := url.PathUnescape(rawVal)
		if err != nil {
			continue
		}
		sid, valid := ParseSessionID(decoded)
		if !valid {
			continue
		}
		if key == "sp.session.id" {
			return sid, true
		}
		if fallback == "" {
			fallback = sid
		}
	}
	if fallback != "" {
		return fallback, true
	}
	return "", false
}

func stripNestedJSONPrefix(s string) string {
	return strings.TrimPrefix(s, "sp.json:")
}

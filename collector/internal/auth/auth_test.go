package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testSecret = "collector-test-hmac-secret"
	testWS1    = "550e8400-e29b-41d4-a716-446655440000"
	testWS2    = "660e8400-e29b-41d4-a716-446655440000"
)

func mintJWT(t *testing.T, secret string, claims map[string]any) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(bodyBytes)
	signing := hdr + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signing + "." + sig
}

func TestParseInboundAuthContracts(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	verifier := NewVerifier(testSecret)

	t.Run("rejects X-Softprobe-Capture-Id with InvalidArgument", func(t *testing.T) {
		md := client.NewMetadata(map[string][]string{
			HeaderCaptureID: {"cap-legacy-1"},
			HeaderAuthz:     {"Bearer tok-1"},
		})
		_, err := ParseInbound(md, verifier, now)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument, got %v", err)
		}
	})

	t.Run("per-second JWT exp rotation shares TenantScopeKey while preserving distinct AuthEnvelope", func(t *testing.T) {
		jwt1 := mintJWT(t, testSecret, map[string]any{
			"iss":          "softprobe-edge",
			"aud":          "sp-backend",
			"sub":          "agent-key",
			"workspace_id": strings.ToUpper(testWS1),
			"agent_id":     "agent-ota",
			"capabilities": []string{"ingest"},
			"exp":          now.Unix() + 300,
		})
		jwt2 := mintJWT(t, testSecret, map[string]any{
			"iss":      "softprobe-edge",
			"aud":      []string{"other", "sp-backend"},
			"sub":      "agent-key",
			"wid":      testWS1,
			"agent_id": "agent-ota",
			"cap":      []string{"ingest"},
			"exp":      now.Unix() + 301,
		})

		env1, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAssertion: {jwt1}}), verifier, now)
		if err != nil {
			t.Fatalf("env1 unexpected err: %v", err)
		}
		env2, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAssertion: {jwt2}}), verifier, now)
		if err != nil {
			t.Fatalf("env2 unexpected err: %v", err)
		}
		if env1.Scope != env2.Scope {
			t.Fatalf("expected identical TenantScopeKey across exp rotation: %+v vs %+v", env1.Scope, env2.Scope)
		}
		if env1.Scope.WorkspaceID != testWS1 || env1.Scope.AgentID != "agent-ota" {
			t.Fatalf("unexpected scope fields: %+v", env1.Scope)
		}
		if env1 == env2 {
			t.Fatalf("AuthEnvelope must keep distinct wire JWTs separate")
		}
	})

	t.Run("enforces agent ingest capability and workspace UUID", func(t *testing.T) {
		noIngest := mintJWT(t, testSecret, map[string]any{
			"iss":          "softprobe-edge",
			"aud":          "sp-backend",
			"sub":          "agent-key",
			"workspace_id": testWS1,
			"agent_id":     "agent-ota",
			"capabilities": []string{"query"},
			"exp":          now.Unix() + 60,
		})
		_, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAssertion: {noIngest}}), verifier, now)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied for missing ingest capability, got %v", err)
		}

		badWS := mintJWT(t, testSecret, map[string]any{
			"iss":          "softprobe-edge",
			"aud":          "sp-backend",
			"sub":          "user-1",
			"workspace_id": "not-a-uuid",
			"exp":          now.Unix() + 60,
		})
		_, err = ParseInbound(client.NewMetadata(map[string][]string{HeaderAssertion: {badWS}}), verifier, now)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied for non-UUID workspace_id, got %v", err)
		}
	})

	t.Run("rejects missing auth, expired JWT, bad signature, and direct spk_ keys", func(t *testing.T) {
		if _, err := ParseInbound(client.NewMetadata(nil), verifier, now); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for empty metadata, got %v", err)
		}

		expired := mintJWT(t, testSecret, map[string]any{
			"iss":          "softprobe-edge",
			"aud":          "sp-backend",
			"sub":          "user-1",
			"workspace_id": testWS1,
			"exp":          now.Unix() - 1,
		})
		if _, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAssertion: {expired}}), verifier, now); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for expired assertion, got %v", err)
		}

		if _, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAuthz: {"Bearer " + expired}}), verifier, now); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for expired Bearer JWT instead of ScopeBearerHash fallthrough, got %v", err)
		}

		if _, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAuthz: {"Bearer a.b.forged"}}), verifier, now); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for forged 3-part Bearer JWT, got %v", err)
		}

		if _, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAuthz: {"Bearer spk_live_secret"}}), verifier, now); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for direct spk_ bearer key, got %v", err)
		}
	})

	t.Run("isolates distinct opaque bearer tokens via ScopeBearerHash", func(t *testing.T) {
		envA, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAuthz: {"Bearer token-alpha"}}), verifier, now)
		if err != nil {
			t.Fatalf("token-alpha err: %v", err)
		}
		envB, err := ParseInbound(client.NewMetadata(map[string][]string{HeaderAuthz: {"Bearer token-beta"}}), verifier, now)
		if err != nil {
			t.Fatalf("token-beta err: %v", err)
		}
		if envA.Scope == envB.Scope {
			t.Fatalf("distinct bearer tokens must not share TenantScopeKey")
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSoftprobeAuthRoundTripperFailsClosedAndOmitsEmptyHeaders(t *testing.T) {
	ext := &authExtension{}
	var captured http.Header
	rt, err := ext.RoundTripper(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		captured = req.Header.Clone()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, nil
	}))
	if err != nil {
		t.Fatalf("RoundTripper: %v", err)
	}

	emptyReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://thelake/v1/traces", nil)
	if _, err := rt.RoundTrip(emptyReq); err == nil {
		t.Fatalf("expected fail-closed error when auth metadata is missing")
	}

	env := AuthEnvelope{
		Scope:     TenantScopeKey{Kind: ScopeAssertion, WorkspaceID: testWS2, AgentID: "ag-1"},
		Assertion: "jwt-token-xyz",
	}
	authedCtx := env.ExportContext(context.Background())
	req, _ := http.NewRequestWithContext(authedCtx, http.MethodPost, "http://thelake/v1/traces", nil)
	req.Header.Set("Authorization", "")
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}
	if got := captured.Get("X-Softprobe-Assertion"); got != "jwt-token-xyz" {
		t.Fatalf("expected X-Softprobe-Assertion, got %q", got)
	}
	if _, hasAuthz := captured["Authorization"]; hasAuthz {
		t.Fatalf("must not emit empty Authorization header, got %v", captured["Authorization"])
	}
}

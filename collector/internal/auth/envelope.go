package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	HeaderAssertion   = "x-softprobe-assertion"
	HeaderAuthz       = "authorization"
	HeaderWorkspaceID = "x-softprobe-workspace-id"
	HeaderScopeOrgID  = "x-scope-orgid"
	HeaderCaptureID   = "x-softprobe-capture-id"
	HeaderBaggage     = "baggage"

	defaultIss = "softprobe-edge"
	defaultAud = "sp-backend"
)

type ScopeKind uint8

const (
	ScopeAssertion ScopeKind = iota + 1
	ScopeBearerHash
)

// TenantScopeKey identifies the tenant and agent boundary for trace session correlation.
// Per-request JWT expiration timestamps are excluded so consecutive requests from the same
// workspace and agent share correlation state without unbounded key growth.
type TenantScopeKey struct {
	Kind           ScopeKind
	WorkspaceID    string
	AgentID        string
	CredentialHash [32]byte
}

// AuthEnvelope stores the verified inbound headers for forwarding to thelake.
// All fields are comparable values so pending batches group strictly by exact credential match.
type AuthEnvelope struct {
	Scope           TenantScopeKey
	Assertion       string
	Authorization   string
	WorkspaceHeader string
	ScopeOrgID      string
	ExpiresAtUnix   int64
}

type Verifier struct {
	secret []byte
}

func NewVerifier(hmacSecret string) *Verifier {
	trimmed := strings.TrimSpace(hmacSecret)
	if trimmed == "" {
		return &Verifier{}
	}
	return &Verifier{secret: []byte(trimmed)}
}

func (v *Verifier) HasSecret() bool {
	return v != nil && len(v.secret) > 0
}

type rawClaims struct {
	Iss          string          `json:"iss"`
	Aud          json.RawMessage `json:"aud"`
	Sub          string          `json:"sub"`
	WorkspaceID  string          `json:"workspace_id"`
	Wid          string          `json:"wid"`
	Capabilities *[]string       `json:"capabilities"`
	Cap          *[]string       `json:"cap"`
	Exp          *int64          `json:"exp"`
	AgentID      *string         `json:"agent_id"`
	AgentName    *string         `json:"agent_name"`
}

func ParseInbound(md client.Metadata, verifier *Verifier, now time.Time) (AuthEnvelope, error) {
	if rawCap := firstTrimmed(md.Get(HeaderCaptureID)); rawCap != "" {
		return AuthEnvelope{}, status.Error(
			codes.InvalidArgument,
			"X-Softprobe-Capture-Id is unsupported; propagate W3C traceparent or baggage",
		)
	}

	wsHeader := firstTrimmed(md.Get(HeaderWorkspaceID))
	scopeOrg := firstTrimmed(md.Get(HeaderScopeOrgID))

	if rawAssertion := firstTrimmed(md.Get(HeaderAssertion)); rawAssertion != "" {
		scope, expUnix, err := verifyAssertionAndScope(rawAssertion, verifier, now.Unix())
		if err != nil {
			return AuthEnvelope{}, err
		}
		return AuthEnvelope{
			Scope:           scope,
			Assertion:       rawAssertion,
			WorkspaceHeader: wsHeader,
			ScopeOrgID:      scopeOrg,
			ExpiresAtUnix:   expUnix,
		}, nil
	}

	rawAuth := firstTrimmed(md.Get(HeaderAuthz))
	token, ok := parseBearer(rawAuth)
	if !ok {
		return AuthEnvelope{}, status.Error(codes.Unauthenticated, "missing or invalid Authorization header")
	}

	if strings.Count(token, ".") == 2 {
		if !verifier.HasSecret() {
			return AuthEnvelope{}, status.Error(codes.Unauthenticated, "assertion HMAC secret is not configured")
		}
		claims, err := verifyJWT(token, verifier.secret, now.Unix())
		if err != nil {
			return AuthEnvelope{}, status.Error(codes.Unauthenticated, err.Error())
		}
		scope, expUnix, scopeErr := authorizeClaims(claims)
		if scopeErr != nil {
			return AuthEnvelope{}, scopeErr
		}
		return AuthEnvelope{
			Scope:           scope,
			Authorization:   rawAuth,
			WorkspaceHeader: wsHeader,
			ScopeOrgID:      scopeOrg,
			ExpiresAtUnix:   expUnix,
		}, nil
	}

	if strings.HasPrefix(token, "spk_") {
		return AuthEnvelope{}, status.Error(
			codes.Unauthenticated,
			"Explorer API keys must be exchanged at the gateway before reaching the collector",
		)
	}

	hashInput := rawAuth + "\x00" + wsHeader + "\x00" + scopeOrg
	return AuthEnvelope{
		Scope: TenantScopeKey{
			Kind:           ScopeBearerHash,
			CredentialHash: sha256.Sum256([]byte(hashInput)),
		},
		Authorization:   rawAuth,
		WorkspaceHeader: wsHeader,
		ScopeOrgID:      scopeOrg,
	}, nil
}

func (env AuthEnvelope) ExportContext(parent context.Context) context.Context {
	headers := make(map[string][]string, 4)
	if env.Assertion != "" {
		headers[HeaderAssertion] = []string{env.Assertion}
	}
	if env.Authorization != "" {
		headers[HeaderAuthz] = []string{env.Authorization}
	}
	if env.WorkspaceHeader != "" {
		headers[HeaderWorkspaceID] = []string{env.WorkspaceHeader}
	}
	if env.ScopeOrgID != "" {
		headers[HeaderScopeOrgID] = []string{env.ScopeOrgID}
	}
	return client.NewContext(parent, client.Info{
		Metadata: client.NewMetadata(headers),
	})
}

func verifyAssertionAndScope(token string, verifier *Verifier, nowSec int64) (TenantScopeKey, int64, error) {
	if !verifier.HasSecret() {
		return TenantScopeKey{}, 0, status.Error(codes.Unauthenticated, "assertion HMAC secret is not configured")
	}
	claims, err := verifyJWT(token, verifier.secret, nowSec)
	if err != nil {
		return TenantScopeKey{}, 0, status.Error(codes.Unauthenticated, err.Error())
	}
	return authorizeClaims(claims)
}

func verifyJWT(token string, secret []byte, nowSec int64) (*rawClaims, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed assertion jwt")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid assertion header encoding")
	}
	var hdr struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &hdr); err != nil || hdr.Alg != "HS256" {
		return nil, fmt.Errorf("unsupported assertion alg")
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSig := mac.Sum(nil)
	gotSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(expectedSig, gotSig) {
		return nil, fmt.Errorf("invalid assertion signature")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid assertion payload encoding")
	}
	var claims rawClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("invalid assertion payload json")
	}
	if claims.Iss != defaultIss {
		return nil, fmt.Errorf("invalid assertion issuer")
	}
	if !audIncludes(claims.Aud, defaultAud) {
		return nil, fmt.Errorf("invalid assertion audience")
	}
	if strings.TrimSpace(claims.Sub) == "" {
		return nil, fmt.Errorf("empty assertion subject")
	}
	if claims.Exp != nil && *claims.Exp < nowSec {
		return nil, fmt.Errorf("assertion expired")
	}
	return &claims, nil
}

func authorizeClaims(claims *rawClaims) (TenantScopeKey, int64, error) {
	hasAgentClaim := claims.AgentID != nil || claims.AgentName != nil || claims.Capabilities != nil || claims.Cap != nil
	var agentID string
	if claims.AgentID != nil {
		agentID = strings.TrimSpace(*claims.AgentID)
	}
	if hasAgentClaim {
		if agentID == "" {
			return TenantScopeKey{}, 0, status.Error(codes.PermissionDenied, "agent assertion missing agent_id")
		}
		caps := claims.Capabilities
		if caps == nil {
			caps = claims.Cap
		}
		hasIngest := false
		if caps != nil {
			for _, c := range *caps {
				if c == "ingest" {
					hasIngest = true
					break
				}
			}
		}
		if !hasIngest {
			return TenantScopeKey{}, 0, status.Error(codes.PermissionDenied, "agent assertion lacks ingest capability")
		}
	}

	rawWS := strings.TrimSpace(claims.WorkspaceID)
	if rawWS == "" {
		rawWS = strings.TrimSpace(claims.Wid)
	}
	wsID, ok := normalizeUUID(rawWS)
	if !ok {
		return TenantScopeKey{}, 0, status.Error(codes.PermissionDenied, "workspace_id must be a valid UUID")
	}

	var expUnix int64
	if claims.Exp != nil {
		expUnix = *claims.Exp
	}
	return TenantScopeKey{
		Kind:        ScopeAssertion,
		WorkspaceID: wsID,
		AgentID:     agentID,
	}, expUnix, nil
}

func audIncludes(raw json.RawMessage, expected string) bool {
	if len(raw) == 0 {
		return false
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return single == expected
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		for _, item := range many {
			if item == expected {
				return true
			}
		}
	}
	return false
}

func normalizeUUID(raw string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	if len(s) != 36 {
		return "", false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return "", false
	}
	hexClean := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	if _, err := hex.DecodeString(hexClean); err != nil {
		return "", false
	}
	return s, true
}

func parseBearer(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "Bearer ") {
		return "", false
	}
	tok := strings.TrimSpace(strings.TrimPrefix(trimmed, "Bearer "))
	if tok == "" {
		return "", false
	}
	return tok, true
}

func firstTrimmed(vals []string) string {
	for _, v := range vals {
		if t := strings.TrimSpace(v); t != "" {
			return t
		}
	}
	return ""
}

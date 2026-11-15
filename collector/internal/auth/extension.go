package auth

import (
	"context"
	"errors"
	"net/http"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
)

var extensionType = component.MustNewType("softprobe_auth")

type ExtensionConfig struct{}

func NewExtensionFactory() extension.Factory {
	return extension.NewFactory(
		extensionType,
		func() component.Config { return &ExtensionConfig{} },
		func(context.Context, extension.Settings, component.Config) (extension.Extension, error) {
			return &authExtension{}, nil
		},
		component.StabilityLevelBeta,
	)
}

type authExtension struct{}

func (e *authExtension) Start(context.Context, component.Host) error { return nil }
func (e *authExtension) Shutdown(context.Context) error              { return nil }

func (e *authExtension) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	if base == nil {
		base = http.DefaultTransport
	}
	return &headerRoundTripper{base: base}, nil
}

type headerRoundTripper struct {
	base http.RoundTripper
}

func (rt *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	md := client.FromContext(req.Context()).Metadata
	assertion := firstTrimmed(md.Get(HeaderAssertion))
	authz := firstTrimmed(md.Get(HeaderAuthz))
	if assertion == "" && authz == "" {
		return nil, errors.New("softprobe_auth: outbound request missing both X-Softprobe-Assertion and Authorization")
	}

	cloned := req.Clone(req.Context())
	cloned.Header.Del("X-Softprobe-Assertion")
	cloned.Header.Del("Authorization")
	cloned.Header.Del("X-Softprobe-Workspace-Id")
	cloned.Header.Del("X-Scope-OrgID")

	if assertion != "" {
		cloned.Header.Set("X-Softprobe-Assertion", assertion)
	}
	if authz != "" {
		cloned.Header.Set("Authorization", authz)
	}
	if ws := firstTrimmed(md.Get(HeaderWorkspaceID)); ws != "" {
		cloned.Header.Set("X-Softprobe-Workspace-Id", ws)
	}
	if org := firstTrimmed(md.Get(HeaderScopeOrgID)); org != "" {
		cloned.Header.Set("X-Scope-OrgID", org)
	}
	return rt.base.RoundTrip(cloned)
}

var _ extensionauth.HTTPClient = (*authExtension)(nil)

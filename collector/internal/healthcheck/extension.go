package healthcheck

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var extensionType = component.MustNewType("health_check")

type Config struct {
	Endpoint string `mapstructure:"endpoint"`
}

func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("health_check endpoint must not be empty")
	}
	return nil
}

func NewFactory() extension.Factory {
	return extension.NewFactory(
		extensionType,
		func() component.Config {
			return &Config{Endpoint: "0.0.0.0:13133"}
		},
		func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
			return &serverExtension{cfg: cfg.(*Config)}, nil
		},
		component.StabilityLevelBeta,
	)
}

type serverExtension struct {
	cfg *Config
	srv *http.Server
}

func (s *serverExtension) Start(_ context.Context, _ component.Host) error {
	ln, err := net.Listen("tcp", s.cfg.Endpoint)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
	mux.HandleFunc("/", handler)
	mux.HandleFunc("/health", handler)
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

func (s *serverExtension) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

package processor

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/softprobe/thelake/collector/internal/auth"
	"github.com/softprobe/thelake/collector/internal/correlation"
)

var processorType = component.MustNewType("sessioncorrelation")

type Config struct {
	AssertionHMACSecret       string        `mapstructure:"assertion_hmac_secret"`
	PendingWindow             time.Duration `mapstructure:"pending_window"`
	ResolvedTTL               time.Duration `mapstructure:"resolved_ttl"`
	ShardCount                int           `mapstructure:"shard_count"`
	MaxResolvedTracesPerScope int           `mapstructure:"max_resolved_traces_per_scope"`
	MaxResolvedTracesGlobal   int           `mapstructure:"max_resolved_traces_global"`
	MaxPendingSpansPerScope   int           `mapstructure:"max_pending_spans_per_scope"`
	MaxPendingSpansGlobal     int           `mapstructure:"max_pending_spans_global"`
	MaxPendingSpansPerTrace   int           `mapstructure:"max_pending_spans_per_trace"`
}

func (cfg *Config) Validate() error {
	if cfg.PendingWindow < 0 {
		return errors.New("pending_window must not be negative")
	}
	if cfg.ResolvedTTL <= 0 {
		return errors.New("resolved_ttl must be positive")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		PendingWindow:             2 * time.Second,
		ResolvedTTL:               10 * time.Minute,
		ShardCount:                64,
		MaxResolvedTracesPerScope: 4096,
		MaxResolvedTracesGlobal:   65536,
		MaxPendingSpansPerScope:   4096,
		MaxPendingSpansGlobal:     32768,
		MaxPendingSpansPerTrace:   256,
	}
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		processorType,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, component.StabilityLevelBeta),
	)
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	c := cfg.(*Config)
	tick := c.PendingWindow / 10
	if tick <= 0 || tick > 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}

	bgCtx, cancelBg := context.WithCancel(context.Background())
	return &sessionCorrelationProcessor{
		logger:   set.Logger,
		next:     nextConsumer,
		verifier: auth.NewVerifier(c.AssertionHMACSecret),
		engine: correlation.NewEngine(correlation.Config{
			PendingWindow:             c.PendingWindow,
			ResolvedTTL:               c.ResolvedTTL,
			ShardCount:                c.ShardCount,
			MaxResolvedTracesPerScope: c.MaxResolvedTracesPerScope,
			MaxResolvedTracesGlobal:   c.MaxResolvedTracesGlobal,
			MaxPendingSpansPerScope:   c.MaxPendingSpansPerScope,
			MaxPendingSpansGlobal:     c.MaxPendingSpansGlobal,
			MaxPendingSpansPerTrace:   c.MaxPendingSpansPerTrace,
		}),
		tickEvery:   tick,
		exportBgCtx: bgCtx,
		cancelBg:    cancelBg,
		stopCh:      make(chan struct{}),
	}, nil
}

type sessionCorrelationProcessor struct {
	logger      *zap.Logger
	next        consumer.Traces
	verifier    *auth.Verifier
	engine      *correlation.Engine
	tickEvery   time.Duration
	exportBgCtx context.Context
	cancelBg    context.CancelFunc
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func (p *sessionCorrelationProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *sessionCorrelationProcessor) Start(context.Context, component.Host) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.tickEvery)
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case now := <-ticker.C:
				batches := p.engine.DrainExpired(now)
				for _, batch := range batches {
					exportCtx, cancel := context.WithTimeout(batch.Auth.ExportContext(p.exportBgCtx), 30*time.Second)
					if err := p.next.ConsumeTraces(exportCtx, batch.Traces); err != nil && p.logger != nil {
						p.logger.Warn("failed to export expired pending trace batch", zap.Error(err))
					}
					cancel()
				}
			}
		}
	}()
	return nil
}

func (p *sessionCorrelationProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	now := time.Now()
	md := client.FromContext(ctx).Metadata
	env, err := auth.ParseInbound(md, p.verifier, now)
	if err != nil {
		return err
	}
	reqBaggage, _ := correlation.ExtractMetadataBaggage(md.Get(auth.HeaderBaggage))

	decision, err := p.engine.IngestWithBaggage(now, env, reqBaggage, td)
	if err != nil {
		return err
	}

	for _, batch := range decision.Immediate {
		if batch.Auth == env {
			if err := p.next.ConsumeTraces(batch.Auth.ExportContext(ctx), batch.Traces); err != nil {
				return err
			}
			continue
		}
		exportCtx, cancel := context.WithTimeout(batch.Auth.ExportContext(p.exportBgCtx), 30*time.Second)
		if err := p.next.ConsumeTraces(exportCtx, batch.Traces); err != nil && p.logger != nil {
			p.logger.Warn("failed to export correlated sibling trace batch", zap.Error(err))
		}
		cancel()
	}
	return nil
}

func (p *sessionCorrelationProcessor) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.wg.Wait()

	var firstErr error
	for _, batch := range p.engine.DrainAll() {
		if err := p.next.ConsumeTraces(batch.Auth.ExportContext(ctx), batch.Traces); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.cancelBg()
	return firstErr
}

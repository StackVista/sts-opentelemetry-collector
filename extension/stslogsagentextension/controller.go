package stslogsagentextension

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	_ logsagent.Controller                  = (*controller)(nil)
	_ extensioncapabilities.PipelineWatcher = (*controller)(nil)
	_ extensioncapabilities.ConfigWatcher   = (*controller)(nil)
	_ componentstatus.Watcher               = (*controller)(nil)
)

type controller struct {
	cfg    Config
	set    extension.Settings
	mu     sync.Mutex
	mode   logsagent.Mode
	bounds logsagent.PipelineConfig

	initialized    bool
	configured     bool
	ready          bool
	stopping       bool
	drainDeadline  time.Time
	observer       logsagent.ExportObserver
	exporterStatus componentstatus.Status
	exporterFailed bool

	now          func() time.Time
	server       *http.Server
	serverDone   chan struct{}
	shutdownOnce sync.Once

	drains        metric.Int64Counter
	drainDuration metric.Float64Histogram
}

func newController(cfg Config, set extension.Settings) (*controller, error) {
	c := &controller{cfg: cfg, set: set, now: time.Now}
	if err := c.initTelemetry(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *controller) Start(ctx context.Context, _ component.Host) error {
	if err := c.startHealth(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.mode = logsagent.PromtailMode
	c.initialized = true
	c.mu.Unlock()
	return nil
}

func (c *controller) NotifyConfig(_ context.Context, conf *confmap.Conf) error {
	bounds, err := logsagent.ValidateEffectivePipelineConfig(conf)
	if err != nil {
		return err
	}
	if bounds.ExtensionID != c.set.ID.String() {
		return errors.New("logs route must reference this controller extension")
	}
	enabled := false
	featuregate.GlobalRegistry().VisitAll(func(g *featuregate.Gate) {
		if g.ID() == "stanza.synchronousLogEmitter" {
			enabled = g.IsEnabled()
		}
	})
	if !enabled {
		return errors.New("logs collection requires --feature-gates=stanza.synchronousLogEmitter")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bounds = bounds
	c.configured = true
	return nil
}

func (c *controller) SelectedMode() logsagent.Mode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode
}

func (c *controller) RegisterExportObserver(observer logsagent.ExportObserver) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initialized || !c.configured || c.observer != nil || observer == nil {
		return errors.New("logs agent must be initialized and configured with exactly one route")
	}
	c.observer = observer
	return nil
}

func (c *controller) DrainDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drainDeadline
}

func (c *controller) RetryBound() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bounds.RetryBound
}

func (c *controller) Ready() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initialized || !c.configured || c.observer == nil || c.stopping {
		return errors.New("logs agent cannot become ready before the route is initialized")
	}
	c.ready = true
	return nil
}

func (c *controller) NotReady() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = false
	c.stopping = true
	if c.drainDeadline.IsZero() {
		c.drainDeadline = c.now().Add(c.bounds.ExportLifetime)
	}
	return nil
}

func (c *controller) ComponentStatusChanged(source *componentstatus.InstanceID, event *componentstatus.Event) {
	if source.Kind() != component.KindExporter {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.bounds.PromtailExporterID
	if source.ComponentID().String() != id {
		return
	}
	c.exporterStatus = event.Status()
	if componentstatus.StatusIsError(event.Status()) {
		c.exporterFailed = true
	}
}

func (c *controller) Shutdown(_ context.Context) error {
	c.shutdownOnce.Do(c.shutdown)
	return nil
}

func (c *controller) shutdown() {
	_ = c.NotReady()
	c.mu.Lock()
	observer := c.observer
	status, failed := c.exporterStatus, c.exporterFailed
	duration := max(0, c.now().Sub(c.drainDeadline.Add(-c.bounds.ExportLifetime)).Seconds())
	c.initialized = false
	c.mu.Unlock()
	if observer != nil {
		snapshot := observer.Snapshot()
		outcome := "completed"
		if snapshot.Failed > 0 || snapshot.DeadlineExpired > 0 || snapshot.DrainRejected > 0 || failed {
			outcome = "failed"
		}
		if snapshot.Outstanding != 0 || status != componentstatus.StatusStopped {
			outcome = "incomplete"
		}
		c.drains.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
		c.drainDuration.Record(context.Background(), duration,
			metric.WithAttributes(attribute.String("outcome", outcome)))
		c.set.Logger.Info("Logs export drain finished", zap.String("outcome", outcome),
			zap.Float64("duration_seconds", duration),
			zap.Int64("outstanding_calls", snapshot.Outstanding), zap.Int64("failed_calls", snapshot.Failed),
			zap.Int64("deadline_expired", snapshot.DeadlineExpired), zap.Int64("drain_rejected", snapshot.DrainRejected),
			zap.Bool("exporter_stopped", status == componentstatus.StatusStopped))
	}
	if c.server != nil {
		c.server.Close()
		<-c.serverDone
	}
}

func (c *controller) startHealth(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", c.cfg.HealthEndpoint)
	if err != nil {
		return errors.New("cannot bind logs agent health endpoint")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		ready := c.initialized && c.configured && c.ready && !c.stopping
		c.mu.Unlock()
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	c.serverDone = make(chan struct{})
	go func() {
		defer close(c.serverDone)
		if err := c.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.mu.Lock()
			c.initialized = false
			c.mu.Unlock()
			c.set.Logger.Error("Logs agent health listener failed")
		}
	}()
	return nil
}

func (c *controller) initTelemetry() error {
	meter := c.set.MeterProvider.Meter("github.com/stackvista/sts-opentelemetry-collector/logsagent")
	var err error
	c.drains, err = meter.Int64Counter("sts_logs_export_drains")
	if err != nil {
		return err
	}
	c.drainDuration, err = meter.Float64Histogram("sts_logs_export_drain_duration", metric.WithUnit("s"))
	if err != nil {
		return err
	}
	return nil
}

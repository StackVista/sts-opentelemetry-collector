package stslogscapabilityextension

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient"
	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient/features"
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

const (
	restartMarker  = "marker"
	restartMessage = "message"
	restartSignal  = "signal"
)

type controller struct {
	cfg    Config
	set    extension.Settings
	mu     sync.Mutex
	mode   logsagent.Mode
	state  restartState
	bounds logsagent.PipelineConfig

	initialized    bool
	configured     bool
	ready          bool
	authFailed     bool
	stopping       bool
	restartPending bool
	candidate      logsagent.Mode
	observations   int
	drainDeadline  time.Time
	observer       logsagent.ExportObserver
	exporterStatus componentstatus.Status
	exporterFailed bool

	now            func() time.Time
	requestRestart func() error
	writeState     func(string, restartState) error
	writeMessage   func(string, []byte) error
	startPolling   func() (*features.Poller, error)
	poller         *features.Poller
	pollDone       chan struct{}
	server         *http.Server
	serverDone     chan struct{}
	closeIdle      func()
	shutdownOnce   sync.Once
	shutdownErr    error

	queries       metric.Int64Counter
	restarts      metric.Int64Counter
	drains        metric.Int64Counter
	drainDuration metric.Float64Histogram
	registration  metric.Registration
}

func newController(cfg Config, set extension.Settings, restart func() error) (*controller, error) {
	c := &controller{
		cfg: cfg, set: set, now: time.Now, requestRestart: restart, writeState: saveState,
		writeMessage: func(path string, data []byte) error { return os.WriteFile(path, data, 0o600) },
	}
	if err := c.initTelemetry(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *controller) Start(ctx context.Context, _ component.Host) error {
	state, err := loadState(c.cfg.StateDirectory)
	if err != nil {
		return err
	}
	opts := openapiclient.ConnectionOptions{
		ReceiverURL: c.cfg.ReceiverURL, APIKey: string(c.cfg.APIKey), ProxyURL: string(c.cfg.ProxyURL),
		InsecureSkipVerify: c.cfg.TLS.InsecureSkipVerify, RequestTimeout: c.cfg.AttemptTimeout,
		UserAgent: c.set.BuildInfo.Command + "/" + c.set.BuildInfo.Version,
	}
	if c.cfg.TLS.CAFile != "" {
		pem, readErr := os.ReadFile(c.cfg.TLS.CAFile)
		if readErr != nil || len(pem) == 0 {
			return errors.New("cannot read logs capability CA bundle")
		}
		opts.CABundlePEM = pem
	}
	api, authCtx, err := openapiclient.NewOpenAPIClient(ctx, opts)
	if err != nil {
		return err
	}
	c.closeIdle = api.GetConfig().HTTPClient.CloseIdleConnections
	client, err := features.NewClient(api.FeaturesAPI, features.QueryOptions{
		Timeout: c.cfg.QueryTimeout, AttemptTimeout: c.cfg.AttemptTimeout, MaxAttempts: c.cfg.MaxAttempts,
		InitialBackoff: c.cfg.InitialBackoff, MaxBackoff: c.cfg.MaxBackoff,
	})
	if err != nil {
		c.closeIdle()
		return err
	}
	if err := c.startHealth(ctx); err != nil {
		c.closeIdle()
		return err
	}
	result := client.FetchFeatures(authCtx)
	c.recordQuery(result)
	mode, err := startupMode(result)
	if err != nil {
		c.server.Close()
		<-c.serverDone
		c.closeIdle()
		return err
	}
	if state.PendingIntent {
		c.set.Logger.Info("Logs capability restart intent observed",
			zap.String("old_mode", string(state.OldMode)), zap.String("requested_mode", string(state.NewMode)),
			zap.Bool("selected_requested_mode", mode == state.NewMode))
		state.PendingIntent = false
		if err := c.writeState(c.cfg.StateDirectory, state); err != nil {
			c.server.Close()
			<-c.serverDone
			c.closeIdle()
			return err
		}
	}
	c.mu.Lock()
	c.state = state
	c.mode = mode
	c.initialized = true
	c.startPolling = func() (*features.Poller, error) {
		return client.StartPolling(context.WithoutCancel(authCtx),
			features.PollOptions{Interval: c.cfg.PollInterval, Jitter: c.cfg.Jitter})
	}
	c.mu.Unlock()
	c.set.Logger.Info("Logs capability selected", zap.String("mode", string(mode)),
		zap.String("query_outcome", string(result.Class)))
	return nil
}

func startupMode(result features.Result) (logsagent.Mode, error) {
	switch result.Class {
	case features.Valid:
		return observedMode(result), nil
	case features.Unsupported, features.Transient, features.Timeout, features.Malformed:
		return logsagent.Legacy, nil
	default:
		return "", fmt.Errorf("logs capability startup failed: %s", result.Class)
	}
}

func observedMode(result features.Result) logsagent.Mode {
	if enabled, _ := result.Features["otel-logs"].(bool); enabled {
		return logsagent.Native
	}
	return logsagent.Legacy
}

func (c *controller) NotifyConfig(_ context.Context, conf *confmap.Conf) error {
	bounds, err := logsagent.ValidateEffectivePipelineConfig(conf)
	if err != nil {
		return err
	}
	if bounds.ExtensionID != c.set.ID.String() {
		return errors.New("logs route must reference this capability extension")
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
		return errors.New("logs capability must be initialized and configured with exactly one route")
	}
	c.observer = observer
	return nil
}

func (c *controller) DrainDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drainDeadline
}

func (c *controller) QueueRetryBound() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bounds.QueueRetryBound
}

func (c *controller) Ready() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initialized || !c.configured || c.observer == nil || c.stopping {
		return errors.New("logs capability cannot become ready before the route is initialized")
	}
	if c.poller != nil {
		return nil
	}
	poller, err := c.startPolling()
	if err != nil {
		return err
	}
	c.poller = poller
	c.pollDone = make(chan struct{})
	c.ready = true
	go func() {
		defer close(c.pollDone)
		for result := range poller.Results() {
			c.observe(result)
		}
		<-poller.Done()
	}()
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
	if c.poller != nil {
		c.poller.Stop()
	}
	return nil
}

func (c *controller) observe(result features.Result) {
	c.recordQuery(result)
	c.mu.Lock()
	if c.stopping || c.restartPending {
		c.mu.Unlock()
		return
	}
	if result.Class == features.Authentication {
		c.authFailed = true
	}
	if result.Class == features.Valid {
		c.authFailed = false
	}
	mode := observedMode(result)
	now := c.now()
	if result.Class != features.Valid || mode == c.mode || now.Before(c.state.LastAttemptAt.Add(c.cfg.RestartCooldown)) {
		c.candidate, c.observations = "", 0
		c.mu.Unlock()
		return
	}
	if c.candidate != mode {
		c.candidate, c.observations = mode, 0
	}
	c.observations++
	if c.observations < c.cfg.StableObservations {
		c.mu.Unlock()
		return
	}
	c.restartPending = true
	c.state = restartState{
		SchemaVersion: 1, LastAttemptAt: now, OldMode: c.mode, NewMode: mode, PendingIntent: true,
	}
	state := c.state
	c.mu.Unlock()

	stage := restartMarker
	err := c.writeState(c.cfg.StateDirectory, state)
	if err == nil {
		stage = restartMessage
		message := fmt.Sprintf("Logs capability changed from %s to %s; requesting graceful restart.\n",
			state.OldMode, state.NewMode)
		err = c.writeMessage(c.cfg.TerminationMessagePath, []byte(message))
	}
	if err == nil {
		stage = restartSignal
		c.mu.Lock()
		if c.stopping {
			c.restartPending = false
			c.mu.Unlock()
			c.set.Logger.Info("Logs capability restart superseded by shutdown")
			return
		}
		err = c.requestRestart()
		c.mu.Unlock()
	}
	if err != nil {
		c.mu.Lock()
		c.restartPending = false
		c.candidate, c.observations = "", 0
		c.mu.Unlock()
		c.restarts.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", stage+"_failed")))
		c.set.Logger.Error("Logs capability restart request failed", zap.String("stage", stage))
		return
	}
	c.restarts.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "requested")))
	c.set.Logger.Info("Logs capability restart requested",
		zap.String("old_mode", string(state.OldMode)), zap.String("new_mode", string(state.NewMode)))
}

func (c *controller) ComponentStatusChanged(source *componentstatus.InstanceID, event *componentstatus.Event) {
	if source.Kind() != component.KindExporter {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.bounds.LegacyExporterID
	if c.mode == logsagent.Native {
		id = c.bounds.NativeExporterID
	}
	if source.ComponentID().String() != id {
		return
	}
	c.exporterStatus = event.Status()
	if componentstatus.StatusIsError(event.Status()) {
		c.exporterFailed = true
	}
}

func (c *controller) Shutdown(_ context.Context) error {
	c.shutdownOnce.Do(func() { c.shutdownErr = c.shutdown() })
	return c.shutdownErr
}

func (c *controller) shutdown() error {
	_ = c.NotReady()
	c.mu.Lock()
	done, observer := c.pollDone, c.observer
	c.mu.Unlock()
	if done != nil {
		<-done
	}
	c.mu.Lock()
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
	if c.closeIdle != nil {
		c.closeIdle()
	}
	if c.registration != nil {
		return c.registration.Unregister()
	}
	return nil
}

func (c *controller) startHealth(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", c.cfg.HealthEndpoint)
	if err != nil {
		return errors.New("cannot bind logs capability health endpoint")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		ready := c.initialized && c.configured && c.ready && !c.authFailed && !c.stopping && !c.restartPending
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
			c.set.Logger.Error("Logs capability health listener failed")
		}
	}()
	return nil
}

func (c *controller) recordQuery(result features.Result) {
	c.queries.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("outcome", string(result.Class))))
	if result.Class == features.Authentication || result.Class == features.Configuration ||
		result.Class == features.Rejected {
		c.set.Logger.Error("Logs capability query rejected", zap.String("outcome", string(result.Class)),
			zap.Int("attempts", result.Attempts))
		return
	}
	c.set.Logger.Debug("Logs capability query completed", zap.String("outcome", string(result.Class)),
		zap.Int("attempts", result.Attempts))
}

func (c *controller) initTelemetry() error {
	meter := c.set.MeterProvider.Meter("github.com/stackvista/sts-opentelemetry-collector/logsagent")
	var err error
	c.queries, err = meter.Int64Counter("sts_logs_capability_queries")
	if err != nil {
		return err
	}
	c.restarts, err = meter.Int64Counter("sts_logs_capability_restarts")
	if err != nil {
		return err
	}
	c.drains, err = meter.Int64Counter("sts_logs_export_drains")
	if err != nil {
		return err
	}
	c.drainDuration, err = meter.Float64Histogram("sts_logs_export_drain_duration", metric.WithUnit("s"))
	if err != nil {
		return err
	}
	state, err := meter.Int64ObservableGauge("sts_logs_capability_state")
	if err != nil {
		return err
	}
	c.registration, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		observer.ObserveInt64(state, int64(c.observations),
			metric.WithAttributes(attribute.String("mode", string(c.mode)), attribute.String("field", "candidate_count")))
		observer.ObserveInt64(state, int64(max(0, c.state.LastAttemptAt.Add(c.cfg.RestartCooldown).Sub(c.now()).Seconds())),
			metric.WithAttributes(attribute.String("mode", string(c.mode)), attribute.String("field", "cooldown_seconds")))
		return nil
	}, state)
	return err
}

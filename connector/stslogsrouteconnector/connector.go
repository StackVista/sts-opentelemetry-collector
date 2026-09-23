package stslogsrouteconnector

import (
	"context"
	"errors"
	"math/bits"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logsConnector struct {
	cfg       Config
	router    connector.LogsRouterAndConsumer
	telemetry telemetry
	account   logsagent.Accounting
	logger    *zap.Logger

	mu         sync.Mutex
	controller logsagent.Controller
	selected   consumer.Logs
	mode       logsagent.Mode
	retryBound time.Duration
	stopping   bool
	slots      chan struct{}
	done       chan struct{}
}

func (c *logsConnector) Start(_ context.Context, host component.Host) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selected != nil || c.stopping {
		return errors.New("logs route already started or stopped")
	}
	controller, ok := host.GetExtensions()[c.cfg.ControllerExtension].(logsagent.Controller)
	if !ok {
		return errors.New("controller_extension does not provide a logs agent controller")
	}
	mode := controller.SelectedMode()
	selectedID := c.cfg.PromtailPipeline
	if mode != logsagent.PromtailMode {
		return errors.New("controller_extension has no valid selected mode")
	}
	bound := controller.RetryBound()
	if bound <= 0 || bound > c.cfg.ExportLifetime {
		return errors.New("validated retry bound must be positive and fit export_lifetime")
	}
	selected, err := c.router.Consumer(selectedID)
	if err != nil {
		return err
	}
	if err := controller.RegisterExportObserver(&c.account); err != nil {
		return err
	}
	c.controller, c.selected, c.mode, c.retryBound = controller, selected, mode, bound
	return nil
}

func (*logsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *logsConnector) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	c.mu.Lock()
	if c.selected == nil || c.stopping {
		c.mu.Unlock()
		return c.reject(ctx, data.LogRecordCount(), "not_running", false)
	}
	selected, controller, mode := c.selected, c.controller, c.mode
	c.mu.Unlock()

	records := data.LogRecordCount()
	if (&plog.ProtoMarshaler{}).LogsSize(data) > c.cfg.MaxRequestBytes {
		return c.reject(ctx, records, "request_too_large", true)
	}
	if oversized := oversizedRecords(data, c.cfg.MaxRecordBytes); oversized != 0 {
		c.telemetry.oversized.Add(ctx, int64(oversized), modeAttributes(mode))
		return c.reject(ctx, records, "record_too_large", true)
	}

	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return c.reject(ctx, records, "not_running", false)
	}
	deadline := time.Now().Add(c.cfg.ExportLifetime)
	if drainDeadline := controller.DrainDeadline(); !drainDeadline.IsZero() {
		if time.Until(drainDeadline) < c.retryBound {
			c.mu.Unlock()
			return c.reject(ctx, records, "drain_budget_insufficient", false)
		}
		if drainDeadline.Before(deadline) {
			deadline = drainDeadline
		}
	}
	select {
	case c.slots <- struct{}{}:
		c.account.Start()
	default:
		c.mu.Unlock()
		return c.reject(ctx, records, "admission_saturated", false)
	}
	c.mu.Unlock()

	exportCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	c.telemetry.outstanding.Add(exportCtx, 1, modeAttributes(mode))
	if selected.Capabilities().MutatesData {
		copyData := plog.NewLogs()
		data.CopyTo(copyData)
		data = copyData
	}
	err := selected.ConsumeLogs(exportCtx, data)
	outcome := classifyOutcome(exportCtx, err)
	if outcome == outcomeDeadline && err == nil {
		err = context.DeadlineExceeded
	}
	draining := !controller.DrainDeadline().IsZero()
	c.telemetry.complete(exportCtx, mode, outcome, draining)
	c.logger.Debug("Logs export completed",
		zap.String("outcome", outcome), zap.String("mode", metricMode(mode)),
		zap.Bool("draining", draining), zap.Int("log_records", records))
	c.mu.Lock()
	c.account.Finish(outcome)
	<-c.slots
	if c.stopping && len(c.slots) == 0 {
		close(c.done)
	}
	c.mu.Unlock()
	return err
}

func (c *logsConnector) reject(ctx context.Context, records int, reason string, permanent bool) error {
	c.mu.Lock()
	mode := c.mode
	draining := c.controller != nil && !c.controller.DrainDeadline().IsZero()
	if draining {
		c.account.RejectDrain()
	}
	c.mu.Unlock()
	c.telemetry.reject(ctx, mode, reason, records)
	c.logger.Debug("Logs export rejected",
		zap.String("outcome", reason), zap.String("mode", metricMode(mode)),
		zap.Bool("draining", draining), zap.Int("log_records", records))
	err := errors.New("logs route pre-export rejection: " + reason)
	if permanent {
		return consumererror.NewPermanent(err)
	}
	return err
}

func (c *logsConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if !c.stopping {
		c.stopping = true
		if len(c.slots) == 0 {
			close(c.done)
		}
	}
	c.mu.Unlock()
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func oversizedRecords(data plog.Logs, limit int) int {
	marshaler := plog.ProtoMarshaler{}
	resource := plog.NewLogs().ResourceLogs().AppendEmpty()
	scope := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	oversized := 0
	for _, rl := range data.ResourceLogs().All() {
		rl.Resource().CopyTo(resource.Resource())
		resource.SetSchemaUrl(rl.SchemaUrl())
		resourceBytes := marshaler.ResourceLogsSize(resource)
		for _, sl := range rl.ScopeLogs().All() {
			sl.Scope().CopyTo(scope.Scope())
			scope.SetSchemaUrl(sl.SchemaUrl())
			scopeBytes := marshaler.ScopeLogsSize(scope)
			for _, lr := range sl.LogRecords().All() {
				recordBytes := messageSize(marshaler.LogRecordSize(lr))
				requestBytes := messageSize(resourceBytes + messageSize(scopeBytes+recordBytes))
				if requestBytes > limit {
					oversized++
				}
			}
		}
	}
	return oversized
}

// Resource, scope and record fields each use a one-byte protobuf tag.
func messageSize(payloadBytes int) int {
	return 1 + (bits.Len(uint(payloadBytes)|1)+6)/7 + payloadBytes
}

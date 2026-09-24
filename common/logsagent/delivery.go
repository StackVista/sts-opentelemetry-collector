package logsagent

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Delivery struct {
	cfg       DeliveryConfig
	telemetry telemetry
	account   Accounting
	logger    *zap.Logger

	mu         sync.Mutex
	controller Controller
	selected   consumer.Logs
	mode       Mode
	retryBound time.Duration
	stopping   bool
	slots      chan struct{}
	done       chan struct{}
}

func NewDelivery(cfg DeliveryConfig, meter metric.MeterProvider, logger *zap.Logger) (*Delivery, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	telemetry, err := newTelemetry(meter)
	if err != nil {
		return nil, err
	}
	return &Delivery{
		cfg:       cfg,
		slots:     make(chan struct{}, cfg.MaxConcurrentCalls),
		done:      make(chan struct{}),
		telemetry: telemetry,
		logger:    logger,
	}, nil
}

func (c *Delivery) Start(controller Controller, selected consumer.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selected != nil || c.stopping {
		return errors.New("logs route already started or stopped")
	}
	if controller == nil {
		return errors.New("controller_extension does not provide a logs agent controller")
	}
	if selected == nil {
		return errors.New("logs delivery requires a selected consumer")
	}
	mode := controller.SelectedMode()
	if mode != PromtailMode && mode != OTELNativeMode {
		return errors.New("controller_extension has no valid selected mode")
	}
	bound := controller.RetryBound()
	if bound <= 0 || bound > c.cfg.ExportLifetime {
		return errors.New("validated retry bound must be positive and fit export_lifetime")
	}
	if err := controller.RegisterExportObserver(&c.account); err != nil {
		return err
	}
	c.controller, c.selected, c.mode, c.retryBound = controller, selected, mode, bound
	return nil
}

func (*Delivery) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *Delivery) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	c.mu.Lock()
	if c.selected == nil || c.stopping {
		c.mu.Unlock()
		return c.reject(ctx, data.LogRecordCount(), "not_running")
	}
	selected, controller, mode := c.selected, c.controller, c.mode
	c.mu.Unlock()

	records := data.LogRecordCount()
	c.mu.Lock()
	if c.stopping {
		c.mu.Unlock()
		return c.reject(ctx, records, "not_running")
	}
	deadline := time.Now().Add(c.cfg.ExportLifetime)
	if drainDeadline := controller.DrainDeadline(); !drainDeadline.IsZero() {
		if time.Until(drainDeadline) < c.retryBound {
			c.mu.Unlock()
			return c.reject(ctx, records, "drain_budget_insufficient")
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
		return c.reject(ctx, records, "admission_saturated")
	}
	c.mu.Unlock()

	exportCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	c.telemetry.outstanding.Add(exportCtx, 1, modeAttributes(mode))
	err := c.deliver(exportCtx, selected, data, mode)
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

func (c *Delivery) reject(ctx context.Context, records int, reason string) error {
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
	return errors.New("logs route pre-export rejection: " + reason)
}

func (c *Delivery) Shutdown(ctx context.Context) error {
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

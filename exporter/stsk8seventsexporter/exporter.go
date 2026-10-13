package stsk8seventsexporter

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/telemetry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/eventmapper"
)

// k8sobjectsreceiver watch-mode record contract: the body holds the watch
// event type and the object, and k8s.resource.name the watched resource.
const (
	attrResourceName = "k8s.resource.name"
	resourceEvents   = "events"
	resourcePods     = "pods"
	watchDeleted     = "DELETED"
)

type eventsExporter struct {
	cfg         *Config
	logger      *zap.Logger
	sender      *intakeSender
	mapper      *eventmapper.Mapper
	producer    string
	defaultHost string
	now         func() time.Time

	mu      sync.Mutex
	pending []telemetry.Event
	dropped int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newEventsExporter(
	cfg *Config, logger *zap.Logger, sender *intakeSender, mapper *eventmapper.Mapper, producer string,
) *eventsExporter {
	defaultHost := cfg.DefaultHost
	if defaultHost == "" {
		defaultHost = producer
	}
	return &eventsExporter{
		cfg: cfg, logger: logger, sender: sender, mapper: mapper,
		producer: producer, defaultHost: defaultHost, now: time.Now,
	}
}

func (e *eventsExporter) start(_ context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(1)
	go e.run(ctx)
	return nil
}

// shutdown sends what is still buffered, within the shutdown context: the watch
// starts from "now" after a restart, so unsent events would never be resent.
func (e *eventsExporter) shutdown(ctx context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	if e.sender == nil {
		return nil
	}
	e.flush(ctx)
	e.sender.client.CloseIdleConnections()
	return nil
}

func (e *eventsExporter) run(ctx context.Context) {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flush(ctx)
		}
	}
}

func (e *eventsExporter) consumeLogs(_ context.Context, logs plog.Logs) error {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		scopes := logs.ResourceLogs().At(i).ScopeLogs()
		for j := 0; j < scopes.Len(); j++ {
			records := scopes.At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				e.enqueue(e.mapRecord(records.At(k))...)
			}
		}
	}
	return nil
}

func (e *eventsExporter) mapRecord(record plog.LogRecord) []telemetry.Event {
	if record.Body().Type() != pcommon.ValueTypeMap {
		return nil
	}
	body := record.Body().Map()
	if watchType, ok := body.Get("type"); !ok || watchType.Str() == watchDeleted {
		return nil
	}
	object, ok := body.Get("object")
	if !ok || object.Type() != pcommon.ValueTypeMap {
		return nil
	}
	resource, _ := record.Attributes().Get(attrResourceName)
	switch resource.Str() {
	case resourceEvents:
		var event v1.Event
		if !e.decode(object.Map(), &event) {
			return nil
		}
		mapped, err := e.mapper.Map(&event)
		if err != nil {
			e.logger.Debug("Skipping Kubernetes event", zap.String("event", event.Namespace+"/"+event.Name), zap.Error(err))
			return nil
		}
		return []telemetry.Event{mapped}
	case resourcePods:
		var pod v1.Pod
		if !e.decode(object.Map(), &pod) {
			return nil
		}
		return e.mapper.MapPod(&pod)
	}
	return nil
}

func (e *eventsExporter) decode(object pcommon.Map, into any) bool {
	raw, err := json.Marshal(object.AsRaw())
	if err == nil {
		err = json.Unmarshal(raw, into)
	}
	if err != nil {
		e.logger.Warn("Skipping Kubernetes object that cannot be decoded", zap.Error(err))
		return false
	}
	return true
}

// enqueue applies what the cluster agent's sender and aggregator do after
// mapping: a default host, a timestamp for events without one, and sorted,
// deduplicated tags.
func (e *eventsExporter) enqueue(events ...telemetry.Event) {
	if len(events) == 0 {
		return
	}
	for i := range events {
		if events[i].Host == "" {
			events[i].Host = e.defaultHost
		}
		if events[i].Ts == 0 {
			events[i].Ts = e.now().Unix()
		}
		slices.Sort(events[i].Tags)
		events[i].Tags = slices.Compact(events[i].Tags)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(e.pending, events...)
	if overflow := len(e.pending) - e.cfg.MaxBufferedEvents; overflow > 0 {
		e.pending = e.pending[overflow:]
		e.dropped += overflow
	}
}

func (e *eventsExporter) flush(ctx context.Context) {
	e.mu.Lock()
	events := e.pending
	e.pending = nil
	dropped := e.dropped
	e.dropped = 0
	e.mu.Unlock()
	if dropped > 0 {
		e.logger.Warn("Dropped Kubernetes events while the intake was unavailable", zap.Int("events", dropped))
	}
	sourceType := e.mapper.SourceType()
	payloads := buildPayloads(e.producer, sourceType, events, e.cfg.MaxEventsPerRequest)
	for i, payload := range payloads {
		err := e.sender.sendWithRetry(ctx, payload)
		if err == nil {
			continue
		}
		var permanent *permanentError
		if errors.As(err, &permanent) {
			e.logger.Warn("The intake rejected Kubernetes events; they are dropped",
				zap.Int("events", len(payload.Events[sourceType])), zap.Error(err))
			continue
		}
		var unsent []telemetry.Event
		for _, rest := range payloads[i:] {
			unsent = append(unsent, rest.Events[sourceType]...)
		}
		e.requeue(unsent)
		if ctx.Err() == nil {
			e.logger.Warn("Failed to send Kubernetes events; retrying at the next interval",
				zap.Int("events", len(unsent)), zap.Error(err))
		}
		return
	}
}

// requeue puts unsent events back ahead of newer ones, keeping the buffer bound.
func (e *eventsExporter) requeue(unsent []telemetry.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(unsent, e.pending...)
	if overflow := len(e.pending) - e.cfg.MaxBufferedEvents; overflow > 0 {
		e.pending = e.pending[overflow:]
		e.dropped += overflow
	}
}

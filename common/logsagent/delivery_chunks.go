package logsagent

import (
	"context"
	"errors"
	"iter"
	"math/bits"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
)

func (c *Delivery) deliver(ctx context.Context, next consumer.Logs, data plog.Logs) error {
	remaining := data.LogRecordCount()
	defer func() { c.telemetry.records(ctx, "unsent", remaining) }()
	oversized := 0
	for record := range sizedRecords(data) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.bytes > c.cfg.MaxRecordBytes {
			oversized++
		}
	}
	remaining -= oversized
	c.telemetry.oversized.Add(ctx, int64(oversized))
	c.telemetry.records(ctx, "dropped", oversized)
	var droppedErr error
	if oversized != 0 {
		droppedErr = consumererror.NewPermanent(errors.New("logs delivery: record_too_large"))
	}
	send := func(chunk plog.Logs) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		count := chunk.LogRecordCount()
		if next.Capabilities().MutatesData {
			copyData := plog.NewLogs()
			chunk.CopyTo(copyData)
			chunk = copyData
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err := next.ConsumeLogs(ctx, chunk)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		remaining -= count
		outcome := "completed"
		if err != nil {
			outcome = "failed"
		}
		c.telemetry.records(ctx, outcome, count)
		return err
	}
	if oversized == 0 && (&plog.ProtoMarshaler{}).LogsSize(data) <= c.cfg.MaxRequestBytes {
		return send(data)
	}
	chunk := plog.NewLogs()
	chunkBytes := 0
	resourceIndex, scopeIndex := -1, -1
	var resource plog.ResourceLogs
	var scope plog.ScopeLogs
	for record := range sizedRecords(data) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.bytes > c.cfg.MaxRecordBytes {
			continue
		}
		if record.bytes > c.cfg.MaxRequestBytes-chunkBytes {
			if err := send(chunk); err != nil {
				return err
			}
			chunk = plog.NewLogs()
			chunkBytes = 0
			resourceIndex, scopeIndex = -1, -1
		}
		if resourceIndex != record.resourceIndex {
			resource = chunk.ResourceLogs().AppendEmpty()
			record.resource.Resource().CopyTo(resource.Resource())
			resource.SetSchemaUrl(record.resource.SchemaUrl())
			resourceIndex, scopeIndex = record.resourceIndex, -1
		}
		if scopeIndex != record.scopeIndex {
			scope = resource.ScopeLogs().AppendEmpty()
			record.scope.Scope().CopyTo(scope.Scope())
			scope.SetSchemaUrl(record.scope.SchemaUrl())
			scopeIndex = record.scopeIndex
		}
		record.log.CopyTo(scope.LogRecords().AppendEmpty())
		// Counting shared metadata per record conservatively bounds each chunk.
		chunkBytes += record.bytes
	}
	if chunk.LogRecordCount() != 0 {
		if err := send(chunk); err != nil {
			return err
		}
	}
	return droppedErr
}

type sizedRecord struct {
	resourceIndex, scopeIndex int
	resource                  plog.ResourceLogs
	scope                     plog.ScopeLogs
	log                       plog.LogRecord
	bytes                     int
}

func sizedRecords(data plog.Logs) iter.Seq[sizedRecord] {
	return func(yield func(sizedRecord) bool) {
		marshaler := plog.ProtoMarshaler{}
		resource := plog.NewResourceLogs()
		scope := plog.NewScopeLogs()
		for i, rl := range data.ResourceLogs().All() {
			rl.Resource().CopyTo(resource.Resource())
			resource.SetSchemaUrl(rl.SchemaUrl())
			resourceBytes := marshaler.ResourceLogsSize(resource)
			for j, sl := range rl.ScopeLogs().All() {
				sl.Scope().CopyTo(scope.Scope())
				scope.SetSchemaUrl(sl.SchemaUrl())
				scopeBytes := marshaler.ScopeLogsSize(scope)
				for _, lr := range sl.LogRecords().All() {
					recordBytes := messageSize(marshaler.LogRecordSize(lr))
					requestBytes := messageSize(resourceBytes + messageSize(scopeBytes+recordBytes))
					if !yield(sizedRecord{i, j, rl, sl, lr, requestBytes}) {
						return
					}
				}
			}
		}
	}
}

// Resource, scope and record fields each use a one-byte protobuf tag.
func messageSize(payloadBytes int) int {
	return 1 + (bits.Len(uint(payloadBytes)|1)+6)/7 + payloadBytes //nolint:gosec // Protobuf sizes are nonnegative.
}

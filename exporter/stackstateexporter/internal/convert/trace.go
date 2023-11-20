package convert

import (
	"context"
	"encoding/binary"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/internal/logger"
	ststracepb "github.com/stackvista/sts-opentelemetry-collector/exporter/stackstateexporter/proto/sts/trace"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type TraceID uint64

type Test [16]byte

func ConvertTrace(ctx context.Context, req ptrace.ResourceSpans) ([]*ststracepb.APITrace, error) {
	logger := logger.ZapLogger(ctx, "convert-trace")
	logger.Debug("Converting ResourceSpans to APITrace", zap.Any("resource-attributes", req.Resource().Attributes()))

	traces := map[TraceID]*ststracepb.APITrace{}
	scopeSpans := req.ScopeSpans()
	for i := 0; i < scopeSpans.Len(); i++ {
		ss := scopeSpans.At(i)
		spans := ss.Spans()
		for j := 0; j < spans.Len(); j++ {
			span := spans.At(j)

			// Group the Spans by their TraceIDs for StS
			tid := span.TraceID()
			traceID := TraceID(binary.BigEndian.Uint64(tid[:]))
			if _, ok := traces[traceID]; !ok {
				traces[traceID] = &ststracepb.APITrace{
					TraceID: uint64(traceID),
					Spans:   []*ststracepb.Span{},
				}
			}
			stsTrace := traces[traceID]

			if span.ParentSpanID().IsEmpty() {
				// Root span, this contains the trace start and end times
				stsTrace.StartTime = int64(span.StartTimestamp())
				stsTrace.EndTime = int64(span.EndTimestamp())
			}

			spanID := span.SpanID()
			stsSpan := &ststracepb.Span{
				Name:     span.Name(),
				TraceID:  uint64(traceID),
				SpanID:   binary.BigEndian.Uint64(spanID[:]),
				Start:    int64(span.StartTimestamp()),
				Duration: int64(span.EndTimestamp() - span.StartTimestamp()),
				Meta:     convertAttributes(span.Attributes()),
			}

			if !span.ParentSpanID().IsEmpty() {
				parentSpanID := span.ParentSpanID()
				stsSpan.ParentID = binary.BigEndian.Uint64(parentSpanID[:])
			}

			stsTrace.Spans = append(stsTrace.Spans, stsSpan)

		}
	}
	tt := []*ststracepb.APITrace{}
	for _, stsTrace := range traces {
		logger.Debug("Sending trace to StS", zap.Uint64("trace_id", stsTrace.TraceID))
		tt = append(tt, stsTrace)
	}

	return tt, nil
}

func convertAttributes(attrs pcommon.Map) map[string]string {
	m := map[string]string{}
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})

	return m
}
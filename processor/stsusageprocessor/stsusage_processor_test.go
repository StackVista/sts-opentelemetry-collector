// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stsusageprocessor_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/testdata"

	"github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor"
	"github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestResourceProcessorAttributesUpsert(t *testing.T) {
	var cfg = &stsusageprocessor.Config{}

	tests := []struct {
		name             string
		config           *stsusageprocessor.Config
		sourceAttributes map[string]string
	}{
		{
			name:             "config_with_attributes_applied_on_nil_resource",
			config:           cfg,
			sourceAttributes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test trace consumer
			ttn := new(consumertest.TracesSink)

			factory := stsusageprocessor.NewFactory()
			rtp, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), tt.config, ttn)
			require.NoError(t, err)
			assert.True(t, rtp.Capabilities().MutatesData)

			sourceTraceData := generateTraceData(tt.sourceAttributes)
			err = rtp.ConsumeTraces(context.Background(), sourceTraceData)
			require.NoError(t, err)
			traces := ttn.AllTraces()
			require.Len(t, traces, 1)
		})
	}
}

func generateTraceData(attributes map[string]string) ptrace.Traces {
	td := testdata.GenerateTraces(1)
	if attributes == nil {
		return td
	}
	resource := td.ResourceSpans().At(0).Resource()
	for k, v := range attributes {
		resource.Attributes().PutStr(k, v)
	}
	return td
}

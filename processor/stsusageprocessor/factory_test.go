// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stsusageprocessor_test

import (
	"context"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor"
	"github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor/internal/metadata"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := stsusageprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.NotNil(t, cfg)
}

func TestCreateProcessor(t *testing.T) {
	factory := stsusageprocessor.NewFactory()
	cfg := &stsusageprocessor.Config{}

	tp, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, tp)

	mp, err := factory.CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	assert.NoError(t, err)
	assert.NotNil(t, mp)
}

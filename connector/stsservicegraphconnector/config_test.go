// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package servicegraphconnector

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"

	"github.com/stackvista/sts-opentelemetry-collector/connector/stsservicegraphconnector/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	// Prepare
	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	factories.Connectors[metadata.Type] = NewFactory()

	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "service-graph-connector-config.yaml"), factories)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t,
		&Config{
			LatencyHistogramBuckets: []time.Duration{1, 2, 3, 4, 5},
			Dimensions:              []string{"dimension-1", "dimension-2"},
			Store: StoreConfig{
				TTL:      time.Second,
				MaxItems: 10,
			},
			CacheLoop:           time.Minute,
			StoreExpirationLoop: 2 * time.Second,
		},
		cfg.Connectors[component.NewID(metadata.Type)],
	)

}

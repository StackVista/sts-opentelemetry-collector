package stsksmreceiver //nolint:testpackage // Drives the receiver with a fake clientset.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestFactory(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, typeStr, factory.Type().String())
	require.NoError(t, componenttest.CheckConfigStruct(factory.CreateDefaultConfig()))

	rcv, err := factory.CreateMetrics(context.Background(), receivertest.NewNopSettings(factory.Type()),
		validConfig(), consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, rcv.Shutdown(context.Background()), "shutdown without start")
}

func TestFactoryRejectsInvalidConfig(t *testing.T) {
	factory := NewFactory()
	_, err := factory.CreateMetrics(context.Background(), receivertest.NewNopSettings(factory.Type()),
		factory.CreateDefaultConfig(), consumertest.NewNop())
	assert.Error(t, err)
}

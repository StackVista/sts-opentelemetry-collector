package servicetokenauthextension_test

import (
	"context"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/servicetokenauthextension"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := servicetokenauthextension.NewFactory()
	expected := &servicetokenauthextension.Config{
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   100,
			ValidTTL:    5 * time.Minute,
			InvalidSize: 100,
		},
		Schema: "StackState",
	}
	actual := factory.CreateDefaultConfig()
	assert.Equal(t, expected, actual)
	assert.NoError(t, componenttest.CheckConfigStruct(actual))
}

func TestCreateExtension_ValidConfig(t *testing.T) {
	cfg := &servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: "http://localhost:8091/authorize",
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 3,
		},
		Schema: "StackState",
	}

	ext, err := servicetokenauthextension.CreateExtension(context.Background(), extensiontest.NewNopSettings(servicetokenauthextension.Type), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, ext)
}

func TestNewFactory(t *testing.T) {
	f := servicetokenauthextension.NewFactory()
	assert.NotNil(t, f)
	assert.Equal(t, f.Type(), servicetokenauthextension.Type)
}
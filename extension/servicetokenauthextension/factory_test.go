package servicetokenauthextension

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

func TestCreateDefaultConfig(t *testing.T) {
	expected := &Config{
		Cache: &CacheSettings{
			ValidSize:   100,
			ValidTtl:    5 * time.Minute,
			InvalidSize: 100,
		},
		Schema: "StackState",
	}
	actual := createDefaultConfig()
	assert.Equal(t, expected, createDefaultConfig())
	assert.NoError(t, componenttest.CheckConfigStruct(actual))
}

func TestCreateExtension_ValidConfig(t *testing.T) {
	cfg := &Config{
		Endpoint: &EndpointSettings{
			Url: "http://localhost:8091/authorize",
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30,
			InvalidSize: 3,
		},
		Schema: "StackState",
	}

	ext, err := createExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, ext)
}

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	assert.NotNil(t, f)
	assert.Equal(t, f.Type(), Type)
}

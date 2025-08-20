package common

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSettingsCache_UpdateAndGetByType(t *testing.T) {
	cache := NewSettingsCache()

	// raw setting
	raw := stsSettingsModel.Setting{Type: "foo"}
	entry := NewSettingEntry(raw)

	// register a converter only for this test
	registerConverter("foo", func(s stsSettingsModel.Setting) (any, error) {
		return "converted-" + string(s.Type), nil
	})

	cache.UpdateSettingsForType("foo", []SettingEntry{entry})

	// first call hydrates via converter
	vals, err := cache.GetSettingsByType("foo")
	require.NoError(t, err)
	require.Equal(t, []any{"converted-foo"}, vals)

	// second call should reuse cached Concrete
	vals2, err := cache.GetSettingsByType("foo")
	require.NoError(t, err)
	require.Equal(t, vals, vals2)
}

func TestSettingsCache_GetAvailableSettingTypes(t *testing.T) {
	cache := NewSettingsCache()
	require.Empty(t, cache.GetAvailableSettingTypes())

	cache.UpdateSettingsForType("bar", []SettingEntry{})
	types := cache.GetAvailableSettingTypes()

	require.Contains(t, types, stsSettingsModel.SettingType("bar"))
}

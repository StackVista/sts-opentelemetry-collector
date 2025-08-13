package settingsproviderextension

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.opentelemetry.io/collector/extension"
)

// SettingsProvider is the interface for components that provide dynamic StackState (internal) settings.
type SettingsProvider interface {
	extension.Extension

	// RegisterForUpdates returns a channel that receives a signal when settings change.
	RegisterForUpdates() <-chan struct{}
	// GetCurrentSettings returns a map (keyed by SettingId) of OtelMapping
	GetCurrentSettings() map[stsSettingsModel.SettingId]stsSettingsModel.OtelMapping
}

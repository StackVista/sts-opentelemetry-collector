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
	// GetCurrentSettings returns a map where the key is the SettingType and the value
	// is a slice of all currently active settings of that type.
	GetCurrentSettings() map[stsSettingsModel.SettingType][]stsSettingsModel.Setting
}

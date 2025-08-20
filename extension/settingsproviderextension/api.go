package settingsproviderextension

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/common"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"go.opentelemetry.io/collector/extension"
)

// StsSettingsProvider is the interface for components that provide dynamic StackState (internal) settings.
type StsSettingsProvider interface {
	extension.Extension

	// RegisterForUpdates returns a channel that receives a signal when settings change.
	RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent
	// Unregister allows a subscriber to unregister for further setting changes.
	Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool

	// GetCurrentSettingsByType using as unexported and untyped as a way to define a contract, but it's not what
	// clients/subscribers should be using because we can't define methods with type parameters on interfaces, e.g.:
	//  - getCurrentSettingsByType[T any](typ stsSettingsModel.SettingType) ([]T, error)
	// Instead, subscribers should use the exported typed accessor GetSettingsAs
	GetCurrentSettingsByType(typ stsSettingsModel.SettingType) ([]any, error) // TODO: don't export?
}

// A helper to ensure subscribers get compile-time checks and deep copies of settings
func GetSettingsAs[T any](p StsSettingsProvider, typ stsSettingsModel.SettingType) ([]T, error) {
	settings, err := p.GetCurrentSettingsByType(typ) // returns []any
	if err != nil {
		return nil, err
	}
	return stsSettingsCommon.CastAndCopySlice[T](settings)
}

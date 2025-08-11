package settingsproviderextension

import (
	"go.opentelemetry.io/collector/extension"
)

// SettingsProvider is the interface for components that provide dynamic StackState (internal) settings.
type SettingsProvider interface {
	extension.Extension

	// RegisterForUpdates returns a channel that receives a signal when settings change.
	RegisterForUpdates() <-chan struct{}
}

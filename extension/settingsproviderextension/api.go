package settingsproviderextension

import (
	"errors"

	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	stsSettingsCore "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"go.opentelemetry.io/collector/extension"
)

// StsSettingsProvider is the main interface that components and subscribers depend on for access to Stack settings.
//
// It intentionally does not expose direct access to raw settings because:
//   - Different setting types have different concrete Go types, which is tricky to encapsulate behind a single
//     type-safe method.
//   - Go interfaces cannot express a generic method signature like
//     SafeGetCurrentSettingsByType[T any](typ SettingType) ([]T, error).
//
// Instead, providers must also implement the internal
// InternalRawSettingsProvider interface, which exposes an untyped
// `UnsafeGetCurrentSettingsByType`. Consumers should not call this
// directly.
//
// To retrieve settings as a concrete type, use the generic helper
// function GetSettingsAs[T], which performs the unsafe cast internally
// and returns a type-safe slice to the caller.
type StsSettingsProvider interface {
	extension.Extension

	// RegisterForUpdates returns a channel that receives a signal when settings change.
	RegisterForUpdates(types ...stsSettingsModel.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error)
	// Unregister allows a subscriber to unregister for further setting changes.
	Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool
}

// InternalRawSettingsProvider is an internal interface that makes it explicit that retrieving settings using
// this interface is not type-safe.
//
// This interface exists solely to work around Go's limitation that interfaces cannot have generic methods.
// All StsSettingsProvider implementations must also implement this interface.
type InternalRawSettingsProvider interface {
	// UnsafeGetCurrentSettingsByType returns settings for a given type as []any.
	//
	// This method is intentionally untyped and unsafe. Callers should use the
	// type-safe GetSettingsAs[T] helper function instead of calling this directly.
	//
	// Returns an error if the setting type is unknown or if retrieval fails.
	UnsafeGetCurrentSettingsByType(typ stsSettingsModel.SettingType) ([]any, error)
}

// GetSettingsAs provides a type-safe way to retrieve settings of a given type
// from a StsSettingsProvider.
//
// This function works around Go's limitation that interfaces cannot have generic methods.
// Internally, it:
//  1. Downcasts the provider to InternalRawSettingsProvider
//  2. Calls the untyped UnsafeGetCurrentSettingsByType method
//  3. Casts the results into a type-safe []T slice
//
// All StsSettingsProvider implementations must also implement InternalRawSettingsProvider
// for this function to work.
//
// Example usage:
//
//	settings, err := GetSettingsAs[OtelComponentMapping](provider, SettingTypeOtelComponentMapping)
//
// Returns an error if the setting type is unknown, retrieval fails, or type casting fails.
func GetSettingsAs[T any](p StsSettingsProvider, typ stsSettingsModel.SettingType) ([]T, error) {
	raw, ok := p.(InternalRawSettingsProvider) // all implementations must also satisfy this
	if !ok {
		return nil, errors.New("unable to cast to InternalRawSettingsProvider")
	}
	vals, err := raw.UnsafeGetCurrentSettingsByType(typ)
	if err != nil {
		return nil, err
	}
	return stsSettingsCore.CastSlice[T](vals)
}

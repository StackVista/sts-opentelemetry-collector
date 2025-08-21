package subscribers

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
)

type SubscriptionService interface {
	Notify(types ...stsSettingsModel.SettingType)
	Register(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent
	Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool
	Shutdown()
}

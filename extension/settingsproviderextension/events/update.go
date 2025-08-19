package events

import stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"

type UpdateSettingsEvent struct {
	Type stsSettingsModel.SettingType
}

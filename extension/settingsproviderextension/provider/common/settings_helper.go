package common

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

func processSetting[T any](setting stsSettingsModel.Setting, processor func(actualSetting interface{}) (T, error)) (T, error) {
	actualSetting, err := setting.ValueByDiscriminator()
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to get setting value: %w", err)
	}
	return processor(actualSetting)
}

// GetSettingId returns the Id of a generic setting
func GetSettingId(setting stsSettingsModel.Setting) (stsSettingsModel.SettingId, error) {
	return processSetting(setting, func(actualSetting interface{}) (stsSettingsModel.SettingId, error) {
		switch v := actualSetting.(type) {
		case stsSettingsModel.OtelComponentMapping:
			return v.Id, nil
		case stsSettingsModel.OtelRelationMapping:
			return v.Id, nil
		default:
			return "", fmt.Errorf("failed to get setting value: %s", v)
		}
	})
}

// GetSettingType returns the SettingType of a generic setting
func GetSettingType(setting stsSettingsModel.Setting) (stsSettingsModel.SettingType, error) {
	return processSetting(setting, func(actualSetting interface{}) (stsSettingsModel.SettingType, error) {
		switch v := actualSetting.(type) {
		case stsSettingsModel.OtelComponentMapping:
			return stsSettingsModel.SettingTypeOtelComponentMapping, nil
		case stsSettingsModel.OtelRelationMapping:
			return stsSettingsModel.SettingTypeOtelRelationMapping, nil
		default:
			return "", fmt.Errorf("unsupported setting value: %s", v)
		}
	})
}

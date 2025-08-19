package common

import (
	"fmt"
	"github.com/mohae/deepcopy"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type ConverterFunc func(stsSettingsModel.Setting) (any, error)

var converters = map[stsSettingsModel.SettingType]func(stsSettingsModel.Setting) (any, error){
	stsSettingsModel.SettingTypeOtelComponentMapping: func(s stsSettingsModel.Setting) (any, error) {
		return s.AsOtelComponentMapping()
	},
	stsSettingsModel.SettingTypeOtelRelationMapping: func(s stsSettingsModel.Setting) (any, error) {
		return s.AsOtelRelationMapping()
	},
}

func ConverterFor(t stsSettingsModel.SettingType) (ConverterFunc, bool) {
	fn, ok := converters[t]
	return fn, ok
}

func DeepCopyAs[T any](val any) (T, error) {
	valCopy := deepcopy.Copy(val)
	typedCopy, ok := valCopy.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("failed to cast deepcopy to expected type")
	}
	return typedCopy, nil
}

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

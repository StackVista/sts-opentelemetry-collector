package core

import (
	"fmt"
	"github.com/mohae/deepcopy"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type ConverterFunc func(stsSettingsModel.Setting) (any, error)

// converters is a private converter map of setting type to concrete setting type converter.
// Add settings that need to be supported by a settings provider to this map.
var converters = map[stsSettingsModel.SettingType]func(stsSettingsModel.Setting) (any, error){
	stsSettingsModel.SettingTypeOtelComponentMapping: func(s stsSettingsModel.Setting) (any, error) {
		return s.AsOtelComponentMapping()
	},

	stsSettingsModel.SettingTypeOtelRelationMapping: func(s stsSettingsModel.Setting) (any, error) {
		return s.AsOtelRelationMapping()
	},
}

// used for testing
func registerConverter(settingType stsSettingsModel.SettingType, converter ConverterFunc) {
	converters[settingType] = converter
}

func ConverterFor(t stsSettingsModel.SettingType) (ConverterFunc, bool) {
	fn, ok := converters[t]
	return fn, ok
}

func CastAndCopySlice[T any](raw []any) ([]T, error) {
	out := make([]T, 0, len(raw))
	for _, v := range raw {
		t, ok := v.(T)
		if !ok {
			return nil, fmt.Errorf("expected element of type %T but got %T", *new(T), v)
		}
		typedCopy, err := DeepCopyAs[T](t)
		if err != nil {
			return nil, err
		}
		out = append(out, typedCopy)
	}
	return out, nil
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

// GetSettingType returns the SettingType of a generic setting
func GetSettingType(setting stsSettingsModel.Setting) (stsSettingsModel.SettingType, error) {
	if _, err := processSetting[stsSettingsModel.OtelComponentMapping](setting); err == nil {
		return stsSettingsModel.SettingTypeOtelComponentMapping, nil
	}
	if _, err := processSetting[stsSettingsModel.OtelRelationMapping](setting); err == nil {
		return stsSettingsModel.SettingTypeOtelRelationMapping, nil
	}
	return "", fmt.Errorf("unsupported setting type")
}

// GetSettingId returns the Id of a generic setting
func GetSettingId(setting stsSettingsModel.Setting) (stsSettingsModel.SettingId, error) {
	if v, err := processSetting[stsSettingsModel.OtelComponentMapping](setting); err == nil {
		return v.Id, nil
	}
	if v, err := processSetting[stsSettingsModel.OtelRelationMapping](setting); err == nil {
		return v.Id, nil
	}
	return "", fmt.Errorf("unsupported setting type")
}

func processSetting[T any](setting stsSettingsModel.Setting) (T, error) {
	var zero T

	val, err := setting.ValueByDiscriminator()
	if err != nil {
		return zero, fmt.Errorf("failed to get setting value: %w", err)
	}

	typed, ok := val.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected type: got %T, want %T", val, zero)
	}

	return typed, nil
}

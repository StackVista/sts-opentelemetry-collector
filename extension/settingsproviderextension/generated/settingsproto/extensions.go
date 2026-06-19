package settingsproto

import (
	"fmt"
	"reflect"
)

// NOTE: these extensions/helpers are not auto-generated

// SizeOfRawSetting returns the size of the underlying raw encoded JSON value.
// The helper exists because the union field on the Setting type is unexported.
func SizeOfRawSetting(s Setting) int64 {
	if s.union == nil {
		return 0
	}
	return int64(len(s.union))
}

type SettingExtension interface {
	GetId() string
	GetIdentifier() string
	GetName() string
	GetExpireAfterMs() int64
	GetSpecificity() *float64
	GetInputSignals() []OtelInputSignal
	GetInput() OtelInput
	GetVars() *[]OtelVariableMapping
}

func (m OtelComponentMapping) GetId() string {
	return m.Id
}

func (m OtelComponentMapping) GetIdentifier() string {
	return m.Identifier
}

func (m OtelComponentMapping) GetName() string {
	return m.Name
}

func (m OtelComponentMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

func (m OtelComponentMapping) GetSpecificity() *float64 {
	if m.Specificity == 0 {
		return nil
	}
	s := m.Specificity
	return &s
}

func (m OtelComponentMapping) GetInputSignals() []OtelInputSignal {
	return m.Input.Signal
}

func (m OtelComponentMapping) GetInput() OtelInput {
	return m.Input
}

func (m OtelComponentMapping) GetVars() *[]OtelVariableMapping {
	return m.Vars
}

func (m OtelRelationMapping) GetId() string {
	return m.Id
}

func (m OtelRelationMapping) GetIdentifier() string {
	return m.Identifier
}

func (m OtelRelationMapping) GetName() string {
	return m.Name
}

func (m OtelRelationMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

func (m OtelRelationMapping) GetSpecificity() *float64 {
	return nil
}

func (m OtelRelationMapping) GetInputSignals() []OtelInputSignal {
	return m.Input.Signal
}

func (m OtelRelationMapping) GetInput() OtelInput {
	return m.Input
}

func (m OtelRelationMapping) GetVars() *[]OtelVariableMapping {
	return m.Vars
}

func GetSettingType[T any]() (SettingType, error) {
	var zero T
	t := reflect.TypeOf(zero)

	switch t {
	case reflect.TypeOf(OtelComponentMapping{}):
		return SettingTypeOtelComponentMapping, nil
	case reflect.TypeOf(OtelRelationMapping{}):
		return SettingTypeOtelRelationMapping, nil
	default:
		return "", fmt.Errorf("unsupported type: %s", t.Name())
	}
}

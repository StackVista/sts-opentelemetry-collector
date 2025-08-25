package core

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSettingsHelper_GetSettingId(t *testing.T) {
	tests := []struct {
		name          string
		setting       stsSettingsModel.Setting
		expectedId    stsSettingsModel.SettingId
		expectedError string
	}{
		{
			name:          "component mapping",
			setting:       newOtelComponentMappingAsSetting("11111"),
			expectedId:    "11111",
			expectedError: "",
		},
		{
			name:          "relation mapping",
			setting:       newOtelRelationMappingAsSetting("22222"),
			expectedId:    "22222",
			expectedError: "",
		},
		{
			name:          "invalid setting",
			setting:       stsSettingsModel.Setting{},
			expectedId:    "",
			expectedError: "unsupported setting type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := GetSettingId(tt.setting)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedId, id)
			}
		})
	}
}

func TestSettingsHelper_GetSettingType(t *testing.T) {
	tests := []struct {
		name          string
		setting       stsSettingsModel.Setting
		expectedType  stsSettingsModel.SettingType
		expectedError string
	}{
		{
			name:          "component mapping",
			setting:       newOtelComponentMappingAsSetting("11111"),
			expectedType:  stsSettingsModel.SettingTypeOtelComponentMapping,
			expectedError: "",
		},
		{
			name:          "relation mapping",
			setting:       newOtelRelationMappingAsSetting("22222"),
			expectedType:  stsSettingsModel.SettingTypeOtelRelationMapping,
			expectedError: "",
		},
		{
			name:          "invalid setting",
			setting:       stsSettingsModel.Setting{},
			expectedType:  "",
			expectedError: "unsupported setting type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settingType, err := GetSettingType(tt.setting)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedType, settingType)
			}
		})
	}
}

func newOtelComponentMapping(id string) stsSettingsModel.OtelComponentMapping {
	return stsSettingsModel.OtelComponentMapping{
		Id:               id,
		CreatedTimeStamp: 2,
		Shard:            0,
		Type:             "OtelComponentMapping",
		Output: stsSettingsModel.OtelComponentMappingOutput{
			DomainIdentifier: newOtelStringExpression("host"),
			DomainName:       *newOtelStringExpression("domain"),
			Identifier:       *newOtelStringExpression("${input.attributes['host.name']}"),
			LayerName:        *newOtelStringExpression("Infrastructure"),
			Name:             *newOtelStringExpression("${input.attributes['service.name']}"),
			TypeName:         *newOtelStringExpression("host-component-type"),
		},
		Conditions: []stsSettingsModel.OtelConditionMapping{
			{
				Action: stsSettingsModel.CREATE,
				Expression: stsSettingsModel.OtelBooleanExpression{
					Expression: "${input.attributes['service.name'] == 'test'}",
				},
			},
		},
	}
}

func newOtelComponentMappingAsSetting(id string) stsSettingsModel.Setting {
	setting := stsSettingsModel.Setting{}
	setting.FromOtelComponentMapping(newOtelComponentMapping(id))
	return setting
}

func newOtelRelationMappingAsSetting(id string) stsSettingsModel.Setting {
	otelRelationMapping := stsSettingsModel.OtelRelationMapping{
		Id:               id,
		CreatedTimeStamp: 2,
		Shard:            0,
		Type:             "OtelRelationMapping",
		Output: stsSettingsModel.OtelRelationMappingOutput{
			SourceId:       *newOtelStringExpression("${input.attributes['host.name']}"),
			TargetId:       *newOtelStringExpression("${input.attributes['service.name']}"),
			TypeIdentifier: newOtelStringExpression("is-hosted-on"),
			TypeName:       *newOtelStringExpression("urn:stackpack:common:relation-type:is-hosted-on"),
		},
	}

	setting := stsSettingsModel.Setting{}
	setting.FromOtelRelationMapping(otelRelationMapping)
	return setting
}

func newOtelStringExpression(expr string) *stsSettingsModel.OtelStringExpression {
	return &stsSettingsModel.OtelStringExpression{
		Expression: expr,
	}
}

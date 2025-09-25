package harness

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

// TestSnapshot is a collection of Kafka records that represent a protocol snapshot.
type TestSnapshot interface {
	Records(topic string) ([]*kgo.Record, error)
	Type() stsSettingsModel.SettingType
}

// ------------------
// -- Generic helpers
// ------------------

// newSnapshotStart returns a SettingsSnapshotStart message key and payload
func newSnapshotStart(settingType stsSettingsModel.SettingType, snapshotID string) (string, []byte, error) {
	msg := stsSettingsModel.SettingsSnapshotStart{
		Id:          snapshotID,
		SettingType: settingType,
	}
	var settingsProtocol stsSettingsModel.SettingsProtocol
	if err := settingsProtocol.FromSettingsSnapshotStart(msg); err != nil {
		return "", nil, fmt.Errorf("failed to convert snapshot start to protocol: %w", err)
	}
	b, err := settingsProtocol.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal protocol: %w", err)
	}
	key := fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStartTypeSettingsSnapshotStart)
	return key, b, nil
}

// newSnapshotStop returns a SettingsSnapshotStop message key and payload
func newSnapshotStop(settingType stsSettingsModel.SettingType, snapshotID string) (string, []byte, error) {
	msg := stsSettingsModel.SettingsSnapshotStop{Id: snapshotID}
	var proto stsSettingsModel.SettingsProtocol
	if err := proto.FromSettingsSnapshotStop(msg); err != nil {
		return "", nil, fmt.Errorf("failed to convert snapshot stop to protocol: %w", err)
	}
	b, err := proto.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal protocol: %w", err)
	}
	key := fmt.Sprintf("%s:%s", settingType, stsSettingsModel.SettingsSnapshotStopTypeSettingsSnapshotStop)
	return key, b, nil
}

// newSettingsEnvelope returns a SettingsEnvelope message key and payload
func newSettingsEnvelope(
	settingType stsSettingsModel.SettingType,
	setting stsSettingsModel.Setting,
	settingID, snapshotID string,
) (string, []byte, error) {
	env := stsSettingsModel.SettingsEnvelope{
		Id:      snapshotID,
		Setting: setting,
	}
	var proto stsSettingsModel.SettingsProtocol
	if err := proto.FromSettingsEnvelope(env); err != nil {
		return "", nil, fmt.Errorf("failed to convert settings envelope to protocol: %w", err)
	}
	b, err := proto.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal protocol: %w", err)
	}
	key := fmt.Sprintf("%s:setting:%s", settingType, settingID)
	return key, b, nil
}

type settingBuilder func() (stsSettingsModel.Setting, string, error)

// buildSnapshot wraps the setting snapshot start, envelope(setting), stop pattern.
func buildSnapshot(
	settingType stsSettingsModel.SettingType,
	snapshotID string,
	topic string,
	build settingBuilder,
) ([]*kgo.Record, error) {
	startKey, startVal, err := newSnapshotStart(settingType, snapshotID)
	if err != nil {
		return nil, err
	}

	setting, settingID, err := build()
	if err != nil {
		return nil, err
	}

	envKey, envVal, err := newSettingsEnvelope(settingType, setting, settingID, snapshotID)
	if err != nil {
		return nil, err
	}

	stopKey, stopVal, err := newSnapshotStop(settingType, snapshotID)
	if err != nil {
		return nil, err
	}

	return []*kgo.Record{
		{Topic: topic, Key: []byte(startKey), Value: startVal},
		{Topic: topic, Key: []byte(envKey), Value: envVal},
		{Topic: topic, Key: []byte(stopKey), Value: stopVal},
	}, nil
}

// -------------------------------------------------------------------
// -- Concrete setting types that satisfy the TestSnapshot interface
// -------------------------------------------------------------------

type OtelComponentMappingSnapshot struct {
	SnapshotID string
	MappingID  string
	Name       string

	Conditions []stsSettingsModel.OtelConditionMapping
	Output     stsSettingsModel.OtelComponentMappingOutput
	Vars       []stsSettingsModel.OtelVariableMapping

	ExpireAfterMs int64
}

func (s OtelComponentMappingSnapshot) Type() stsSettingsModel.SettingType {
	return stsSettingsModel.SettingTypeOtelComponentMapping
}

func (s OtelComponentMappingSnapshot) Records(topic string) ([]*kgo.Record, error) {
	return buildSnapshot(s.Type(), s.SnapshotID, topic, func() (stsSettingsModel.Setting, string, error) {
		mapping := stsSettingsModel.OtelComponentMapping{
			CreatedTimeStamp: time.Now().Unix(),
			ExpireAfterMs:    s.ExpireAfterMs,
			Id:               s.MappingID,
			Name:             s.Name,
			Conditions:       s.Conditions,
			Output:           s.Output,
			Shard:            0,
			Type:             stsSettingsModel.OtelComponentMappingTypeOtelComponentMapping,
		}

		if len(s.Vars) > 0 {
			mapping.Vars = &s.Vars
		}

		var setting stsSettingsModel.Setting
		if err := setting.FromOtelComponentMapping(mapping); err != nil {
			return stsSettingsModel.Setting{}, "", fmt.Errorf("failed to convert component mapping to setting: %w", err)
		}
		return setting, mapping.Id, nil
	})
}

type OtelRelationMappingSnapshot struct {
	SnapshotID string
	MappingID  string

	Conditions []stsSettingsModel.OtelConditionMapping
	Output     stsSettingsModel.OtelRelationMappingOutput
	Vars       []stsSettingsModel.OtelVariableMapping

	ExpireAfterMs int64
}

func (s OtelRelationMappingSnapshot) Type() stsSettingsModel.SettingType {
	return stsSettingsModel.SettingTypeOtelRelationMapping
}

func (s OtelRelationMappingSnapshot) Records(topic string) ([]*kgo.Record, error) {
	return buildSnapshot(s.Type(), s.SnapshotID, topic, func() (stsSettingsModel.Setting, string, error) {
		mapping := stsSettingsModel.OtelRelationMapping{
			CreatedTimeStamp: time.Now().Unix(),
			ExpireAfterMs:    s.ExpireAfterMs,
			Id:               s.MappingID,
			Conditions:       s.Conditions,
			Output:           s.Output,
			Shard:            0,
			Type:             stsSettingsModel.OtelRelationMappingTypeOtelRelationMapping,
		}

		if len(s.Vars) > 0 {
			mapping.Vars = &s.Vars
		}

		var setting stsSettingsModel.Setting
		if err := setting.FromOtelRelationMapping(mapping); err != nil {
			return stsSettingsModel.Setting{}, "", fmt.Errorf("failed to convert relation mapping to setting: %w", err)
		}
		return setting, mapping.Id, nil
	})
}

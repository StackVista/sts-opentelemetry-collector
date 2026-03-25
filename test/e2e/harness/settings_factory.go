package harness

import (
	"fmt"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestSnapshot is a collection of Kafka records that represent a settings protocol snapshot.
type TestSnapshot interface {
	Records(topic string) ([]*kgo.Record, error)
	Type() settingsproto.SettingType
}

// ------------------
// -- Generic helpers
// ------------------

// newSnapshotStart returns a SettingsSnapshotStart message key and payload
func newSnapshotStart(settingType settingsproto.SettingType, snapshotID string) (string, []byte, error) {
	msg := settingsproto.SettingsSnapshotStart{
		Id:          snapshotID,
		SettingType: settingType,
	}
	var settingsProtocol settingsproto.SettingsProtocol
	if err := settingsProtocol.FromSettingsSnapshotStart(msg); err != nil {
		return "", nil, fmt.Errorf("failed to convert snapshot start to protocol: %w", err)
	}
	b, err := settingsProtocol.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal protocol: %w", err)
	}
	key := fmt.Sprintf("%s:%s", settingType, settingsproto.SettingsSnapshotStartTypeSettingsSnapshotStart)
	return key, b, nil
}

// newSnapshotStop returns a SettingsSnapshotStop message key and payload
func newSnapshotStop(settingType settingsproto.SettingType, snapshotID string) (string, []byte, error) {
	msg := settingsproto.SettingsSnapshotStop{Id: snapshotID}
	var proto settingsproto.SettingsProtocol
	if err := proto.FromSettingsSnapshotStop(msg); err != nil {
		return "", nil, fmt.Errorf("failed to convert snapshot stop to protocol: %w", err)
	}
	b, err := proto.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal protocol: %w", err)
	}
	key := fmt.Sprintf("%s:%s", settingType, settingsproto.SettingsSnapshotStopTypeSettingsSnapshotStop)
	return key, b, nil
}

// newSettingsEnvelope returns a SettingsEnvelope message key and payload
func newSettingsEnvelope(
	settingType settingsproto.SettingType,
	setting settingsproto.Setting,
	settingID,
	snapshotID string,
) (string, []byte, error) {
	env := settingsproto.SettingsEnvelope{
		Id:      snapshotID,
		Setting: setting,
	}
	var proto settingsproto.SettingsProtocol
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

type settingBuilder func() (settingsproto.Setting, string, error)

// buildSnapshot wraps the setting snapshot start, envelope(setting1)...envelope(settingN), stop pattern.
func buildSnapshot(
	settingType settingsproto.SettingType,
	snapshotID string,
	topic string,
	builders ...settingBuilder,
) ([]*kgo.Record, error) {
	startKey, startVal, err := newSnapshotStart(settingType, snapshotID)
	if err != nil {
		return nil, err
	}

	records := []*kgo.Record{
		{Topic: topic, Key: []byte(startKey), Value: startVal},
	}

	for _, build := range builders {
		setting, settingID, err := build()
		if err != nil {
			return nil, err
		}

		envKey, envVal, err := newSettingsEnvelope(settingType, setting, settingID, snapshotID)
		if err != nil {
			return nil, err
		}

		records = append(records, &kgo.Record{Topic: topic, Key: []byte(envKey), Value: envVal})
	}

	stopKey, stopVal, err := newSnapshotStop(settingType, snapshotID)
	if err != nil {
		return nil, err
	}
	records = append(records, &kgo.Record{Topic: topic, Key: []byte(stopKey), Value: stopVal})

	return records, nil
}

// -------------------------------------------------------------------
// -- Concrete setting types that satisfy the TestSnapshot interface
// -------------------------------------------------------------------

type OtelComponentMappingSnapshot struct {
	SnapshotID string
	Mappings   []*OtelComponentMappingSpec
}

type OtelComponentMappingSpec struct {
	MappingID         string
	MappingIdentifier string
	Name              string
	Input             settingsproto.OtelInput
	Output            settingsproto.OtelComponentMappingOutput
	Vars              []settingsproto.OtelVariableMapping
	ExpireAfterMs     int64
}

func (s OtelComponentMappingSnapshot) Type() settingsproto.SettingType {
	return settingsproto.SettingTypeOtelComponentMapping
}

func (s OtelComponentMappingSnapshot) Records(topic string) ([]*kgo.Record, error) {
	builders := make([]settingBuilder, 0, len(s.Mappings))

	for _, mapping := range s.Mappings {
		builders = append(builders, func() (settingsproto.Setting, string, error) {
			component := settingsproto.OtelComponentMapping{
				CreatedTimeStamp: time.Now().Unix(),
				ExpireAfterMs:    mapping.ExpireAfterMs,
				Id:               mapping.MappingID,
				Identifier:       mapping.MappingIdentifier,
				Name:             mapping.Name,
				Input:            mapping.Input,
				Output:           mapping.Output,
				Shard:            0,
				Type:             settingsproto.OtelComponentMappingTypeOtelComponentMapping,
			}
			if len(mapping.Vars) > 0 {
				component.Vars = &mapping.Vars
			}

			var setting settingsproto.Setting
			if err := setting.FromOtelComponentMapping(component); err != nil {
				return settingsproto.Setting{}, "", fmt.Errorf("failed to convert component mapping to setting: %w", err)
			}
			return setting, mapping.MappingID, nil
		})
	}

	return buildSnapshot(s.Type(), s.SnapshotID, topic, builders...)
}

type OtelRelationMappingSnapshot struct {
	SnapshotID string
	Mappings   []*OtelRelationMappingSpec
}

type OtelRelationMappingSpec struct {
	MappingID         string
	MappingIdentifier string
	Input             settingsproto.OtelInput
	Output            settingsproto.OtelRelationMappingOutput
	Vars              []settingsproto.OtelVariableMapping
	ExpireAfterMs     int64
}

func (s OtelRelationMappingSnapshot) Type() settingsproto.SettingType {
	return settingsproto.SettingTypeOtelRelationMapping
}

func (s OtelRelationMappingSnapshot) Records(topic string) ([]*kgo.Record, error) {
	builders := make([]settingBuilder, 0, len(s.Mappings))

	for _, mapping := range s.Mappings {
		builders = append(builders, func() (settingsproto.Setting, string, error) {
			relation := settingsproto.OtelRelationMapping{
				CreatedTimeStamp: time.Now().Unix(),
				ExpireAfterMs:    mapping.ExpireAfterMs,
				Id:               mapping.MappingID,
				Identifier:       mapping.MappingIdentifier,
				Input:            mapping.Input,
				Output:           mapping.Output,
				Shard:            0,
				Type:             settingsproto.OtelRelationMappingTypeOtelRelationMapping,
			}

			if len(mapping.Vars) > 0 {
				relation.Vars = &mapping.Vars
			}

			var setting settingsproto.Setting
			if err := setting.FromOtelRelationMapping(relation); err != nil {
				return settingsproto.Setting{}, "", fmt.Errorf("failed to convert relation mapping to setting: %w", err)
			}
			return setting, mapping.MappingID, nil
		})
	}

	return buildSnapshot(s.Type(), s.SnapshotID, topic, builders...)
}

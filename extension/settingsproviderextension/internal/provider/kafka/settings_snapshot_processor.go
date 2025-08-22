package kafka

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"go.uber.org/zap"
	"sync"
)

type InProgressSnapshot struct {
	snapshotId  string // Track the current snapshot Id (uuid) for this type
	settingType stsSettingsModel.SettingType
	settings    []stsSettingsModel.Setting
}

type SettingsSnapshotProcessor interface {
	ProcessSettingsProtocol(settingsProtocol *stsSettingsModel.SettingsProtocol) error
}

type DefaultSettingsSnapshotProcessor struct {
	logger        *zap.Logger
	settingsCache stsSettingsCommon.SettingsCache

	// Mutex for concurrent access to InProgressSnapshots
	snapshotsLock sync.RWMutex
	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the SnapshotStart/SettingsEnvelope/SnapshotStop's 'Id' field).
	InProgressSnapshots map[stsSettingsModel.SettingType]*InProgressSnapshot
}

func NewDefaultSettingsSnapshotProcessor(logger *zap.Logger, cache stsSettingsCommon.SettingsCache) *DefaultSettingsSnapshotProcessor {
	return &DefaultSettingsSnapshotProcessor{
		logger:              logger,
		settingsCache:       cache,
		InProgressSnapshots: make(map[stsSettingsModel.SettingType]*InProgressSnapshot),
	}
}

func (d *DefaultSettingsSnapshotProcessor) ProcessSettingsProtocol(settingsProtocol *stsSettingsModel.SettingsProtocol) error {
	actualMessage, err := settingsProtocol.ValueByDiscriminator()
	if err != nil {
		return fmt.Errorf("error getting settingsProtocol by discriminator: %w", err)
	}

	switch v := actualMessage.(type) {
	case stsSettingsModel.SettingsSnapshotStart:
		return d.handleSnapshotStart(v)
	case stsSettingsModel.SettingsEnvelope:
		return d.handleSettingsEnvelope(v)
	case stsSettingsModel.SettingsSnapshotStop:
		return d.handleSnapshotStop(v)
	default:
		return fmt.Errorf("unknown settingsProtocol type: %T", actualMessage)
	}
}

func (d *DefaultSettingsSnapshotProcessor) handleSnapshotStart(msg stsSettingsModel.SettingsSnapshotStart) error {
	d.snapshotsLock.Lock()
	defer d.snapshotsLock.Unlock()

	// Check if there's already a snapshot in progress for this setting type
	if existingSnapshot, exists := d.InProgressSnapshots[msg.SettingType]; exists {
		d.logger.Warn("Replacing orphaned snapshot with new snapshot.",
			zap.String("settingType", string(msg.SettingType)),
			zap.String("oldSnapshotId", existingSnapshot.snapshotId),
			zap.String("newSnapshotId", msg.Id),
			zap.Int("orphanedSettingsCount", len(existingSnapshot.settings)))
		// TODO: add a metric for orphaned snapshots
	}

	d.logger.Debug("Received snapshot start.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(msg.SettingType)))

	d.InProgressSnapshots[msg.SettingType] = &InProgressSnapshot{
		snapshotId:  msg.Id,
		settingType: msg.SettingType,
		settings:    make([]stsSettingsModel.Setting, 0),
	}
	return nil
}

func (d *DefaultSettingsSnapshotProcessor) handleSettingsEnvelope(msg stsSettingsModel.SettingsEnvelope) error {
	d.snapshotsLock.Lock()
	defer d.snapshotsLock.Unlock()

	// We need to find the snapshot by checking if the envelope's ID matches any in-progress snapshot
	targetSnapshot := d.findSnapshot(msg.Id)

	if targetSnapshot == nil {
		d.logger.Warn("Received an orphan settings envelope for a not in progress snapshot.",
			zap.String("snapshotId", msg.Id))
		// TODO: add a metric for this case
		return nil
	}

	targetSnapshot.settings = append(targetSnapshot.settings, msg.Setting)
	return nil
}

func (d *DefaultSettingsSnapshotProcessor) handleSnapshotStop(msg stsSettingsModel.SettingsSnapshotStop) error {
	d.snapshotsLock.Lock()

	targetSnapshot := d.findSnapshot(msg.Id)

	if targetSnapshot == nil {
		d.snapshotsLock.Unlock()
		d.logger.Warn("Received an orphan snapshot stop for an unknown snapshot.",
			zap.String("snapshotId", msg.Id))
		return nil
	}

	delete(d.InProgressSnapshots, targetSnapshot.settingType)
	d.snapshotsLock.Unlock()

	d.logger.Info("Received snapshot stop. Processing complete snapshot.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(targetSnapshot.settingType)),
		zap.Int("settingCount", len(targetSnapshot.settings)))

	settingEntries := make([]stsSettingsCommon.SettingEntry, len(targetSnapshot.settings))
	for i, s := range targetSnapshot.settings {
		settingEntries[i] = stsSettingsCommon.NewSettingEntry(s)
	}

	d.settingsCache.UpdateSettingsForType(targetSnapshot.settingType, settingEntries)

	return nil
}

func (d *DefaultSettingsSnapshotProcessor) findSnapshot(snapshotId string) *InProgressSnapshot {
	for _, snapshot := range d.InProgressSnapshots {
		if snapshot.snapshotId == snapshotId {
			return snapshot
		}
	}
	return nil
}

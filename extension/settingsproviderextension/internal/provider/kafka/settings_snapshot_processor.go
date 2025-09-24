package kafka

import (
	"context"
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	stsSettingsCommon "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"sync"
)

type InProgressSnapshot struct {
	snapshotID  string // Track the current snapshot Id (uuid) for this type
	settingType stsSettingsModel.SettingType
	settings    []stsSettingsModel.Setting
}

type SettingsSnapshotProcessor interface {
	ProcessSettingsProtocol(settingsProtocol *stsSettingsModel.SettingsProtocol) error
}

type DefaultSettingsSnapshotProcessor struct {
	//nolint:containedctx
	ctx               context.Context
	logger            *zap.Logger
	telemetrySettings component.TelemetrySettings

	// Metrics
	incompleteSnapshotsCounter metric.Int64Counter
	successfulEndsCounter      metric.Int64Counter
	settingsCountHistogram     metric.Int64Histogram
	settingsSizeHistogram      metric.Int64Histogram

	settingsCache stsSettingsCommon.SettingsCache

	// Mutex for concurrent access to InProgressSnapshots
	snapshotsLock sync.RWMutex
	// A map to store snapshots that are currently in the process of being received.
	// The key is the snapshot UUID (from the SnapshotStart/SettingsEnvelope/SnapshotStop's 'Id' field).
	InProgressSnapshots map[stsSettingsModel.SettingType]*InProgressSnapshot
}

func NewDefaultSettingsSnapshotProcessor(
	context context.Context,
	logger *zap.Logger,
	telemetrySettings component.TelemetrySettings,
	cache stsSettingsCommon.SettingsCache,
) (*DefaultSettingsSnapshotProcessor, error) {
	meter := telemetrySettings.MeterProvider.Meter("settings_snapshot_processor")

	incompleteSnapshotsCounter, err := meter.Int64Counter(
		"settings_snapshot_incomplete_total",
		metric.WithDescription("Total number of incomplete snapshots (replaced by new starts)"),
	)
	if err != nil {
		return nil, err
	}

	successfulEndsCounter, err := meter.Int64Counter(
		"settings_snapshot_ends_total",
		metric.WithDescription("Total number of successful snapshot ends processed"),
	)
	if err != nil {
		return nil, err
	}

	settingsCountHistogram, err := meter.Int64Histogram(
		"settings_count_by_type",
		metric.WithDescription("Distribution of setting count by type in completed snapshots"),
		metric.WithUnit("settings"),
	)
	if err != nil {
		return nil, err
	}

	settingsSizeHistogram, err := meter.Int64Histogram(
		"settings_count_by_type",
		metric.WithDescription("Distribution of setting count by type in completed snapshots"),
		metric.WithUnit("settings"),
	)
	if err != nil {
		return nil, err
	}

	return &DefaultSettingsSnapshotProcessor{
		ctx:                        context,
		logger:                     logger,
		telemetrySettings:          telemetrySettings,
		incompleteSnapshotsCounter: incompleteSnapshotsCounter,
		successfulEndsCounter:      successfulEndsCounter,
		settingsCountHistogram:     settingsCountHistogram,
		settingsSizeHistogram:      settingsSizeHistogram,
		settingsCache:              cache,
		InProgressSnapshots:        make(map[stsSettingsModel.SettingType]*InProgressSnapshot),
	}, nil
}

func (d *DefaultSettingsSnapshotProcessor) ProcessSettingsProtocol(
	settingsProtocol *stsSettingsModel.SettingsProtocol,
) error {
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
			zap.String("oldSnapshotId", existingSnapshot.snapshotID),
			zap.String("newSnapshotId", msg.Id),
			zap.Int("orphanedSettingsCount", len(existingSnapshot.settings)))

		d.incIncompleteSnapshotCount(msg.SettingType)
	}

	d.logger.Debug("Received snapshot start.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(msg.SettingType)))

	d.InProgressSnapshots[msg.SettingType] = &InProgressSnapshot{
		snapshotID:  msg.Id,
		settingType: msg.SettingType,
		settings:    make([]stsSettingsModel.Setting, 0),
	}
	return nil
}

func (d *DefaultSettingsSnapshotProcessor) handleSettingsEnvelope(msg stsSettingsModel.SettingsEnvelope) error {
	d.snapshotsLock.Lock()
	defer d.snapshotsLock.Unlock()

	targetSnapshot := d.findSnapshot(msg.Id)

	if targetSnapshot == nil {
		d.logger.Warn("Received an orphan settings envelope for a not in progress snapshot.",
			zap.String("snapshotId", msg.Id))
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

	d.logger.Debug("Received snapshot stop. Processing complete snapshot.",
		zap.String("snapshotId", msg.Id),
		zap.String("settingType", string(targetSnapshot.settingType)),
		zap.Int("settingCount", len(targetSnapshot.settings)))

	settingEntries := make([]stsSettingsCommon.SettingEntry, len(targetSnapshot.settings))
	for i, s := range targetSnapshot.settings {
		settingEntries[i] = stsSettingsCommon.NewSettingEntry(s)
	}

	d.settingsCache.UpdateSettingsForType(targetSnapshot.settingType, settingEntries)

	// Metrics
	d.incCompleteSnapshotCount(targetSnapshot.settingType)
	d.recordSettingsCount(targetSnapshot.settingType, int64(len(targetSnapshot.settings)))
	d.recordSettingsSize(targetSnapshot.settingType, targetSnapshot.settings)

	return nil
}

func (d *DefaultSettingsSnapshotProcessor) findSnapshot(snapshotID string) *InProgressSnapshot {
	for _, snapshot := range d.InProgressSnapshots {
		if snapshot.snapshotID == snapshotID {
			return snapshot
		}
	}
	return nil
}

func (d *DefaultSettingsSnapshotProcessor) incIncompleteSnapshotCount(settingType stsSettingsModel.SettingType) {
	d.incompleteSnapshotsCounter.Add(d.ctx, 1,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
			attribute.String("reason", "replaced_by_new_start"),
		))
}

func (d *DefaultSettingsSnapshotProcessor) incCompleteSnapshotCount(settingType stsSettingsModel.SettingType) {
	d.successfulEndsCounter.Add(d.ctx, 1,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		))
}

func (d *DefaultSettingsSnapshotProcessor) recordSettingsCount(
	settingType stsSettingsModel.SettingType,
	settingsCounts int64,
) {
	d.settingsCountHistogram.Record(d.ctx, settingsCounts,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		))
}

func (d *DefaultSettingsSnapshotProcessor) recordSettingsSize(
	settingType stsSettingsModel.SettingType,
	settings []stsSettingsModel.Setting,
) {
	var settingsSize int64
	for _, s := range settings {
		settingsSize += stsSettingsModel.SizeOfRawSetting(s)
	}

	d.logger.Debug(
		"Recording settings size",
		zap.Int64("settings_size_bytes", settingsSize),
		zap.String("settings_size_kb", fmt.Sprintf("%.2f KB", float64(settingsSize)/1024)),
	)

	d.settingsCountHistogram.Record(d.ctx, settingsSize,
		metric.WithAttributes(
			attribute.String("setting_type", string(settingType)),
		))
}

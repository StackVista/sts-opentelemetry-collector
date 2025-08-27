package kafka

import (
	"context"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsCore "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/internal/core"
	"go.opentelemetry.io/collector/component/componenttest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zaptest"

	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
)

type mockSettingsCache struct {
	mock.Mock
}

func (m *mockSettingsCache) UpdateSettingsForType(t stsSettingsModel.SettingType, entries []stsSettingsCore.SettingEntry) {
	m.Called(t, entries)
}
func (m *mockSettingsCache) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return false
}
func (m *mockSettingsCache) GetAvailableSettingTypes() []stsSettingsModel.SettingType { return nil }
func (m *mockSettingsCache) GetConcreteSettingsByType(settingType stsSettingsModel.SettingType) ([]any, error) {
	return nil, nil
}
func (m *mockSettingsCache) RegisterForUpdates(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return nil
}
func (m *mockSettingsCache) Update(settingsByType stsSettingsCore.SettingsByType) {}
func (m *mockSettingsCache) Shutdown()                                            {}

func newProcessorWithMockCache(t *testing.T) (*DefaultSettingsSnapshotProcessor, *mockSettingsCache) {
	cache := &mockSettingsCache{}
	p, err := NewDefaultSettingsSnapshotProcessor(context.Background(), zaptest.NewLogger(t), componenttest.NewNopTelemetrySettings(), cache)
	assert.NoError(t, err)

	return p, cache
}

func TestHandleSnapshotStart_AddsNewSnapshot(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	start := stsSettingsModel.SettingsSnapshotStart{
		Id:          "snap-1",
		SettingType: "testType",
		Type:        "SettingsSnapshotStart", // explicitly set the discriminator
	}
	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStart(start)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	snap, ok := p.InProgressSnapshots["testType"]
	assert.True(t, ok)
	assert.Equal(t, "snap-1", snap.snapshotId)
	assert.Empty(t, snap.settings)
}

func TestHandleSnapshotStart_ReplacesExistingSnapshot(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	// Insert an existing snapshot manually
	p.InProgressSnapshots["testType"] = &InProgressSnapshot{
		snapshotId: "old-id",
	}

	start := stsSettingsModel.SettingsSnapshotStart{
		Id:          "new-id",
		SettingType: "testType",
		Type:        "SettingsSnapshotStart", // explicitly set the discriminator
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStart(start)
	assert.NoError(t, err)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	snap := p.InProgressSnapshots["testType"]
	assert.Equal(t, "new-id", snap.snapshotId)
}

func TestHandleSettingsEnvelope_AppendsSetting(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	// Seed snapshot
	p.InProgressSnapshots["foo"] = &InProgressSnapshot{
		snapshotId: "snap-1",
		settings:   []stsSettingsModel.Setting{},
	}

	env := stsSettingsModel.SettingsEnvelope{
		Id:      "snap-1",
		Setting: stsSettingsModel.Setting{Type: "foo"},
		Type:    "SettingsEnvelope", // explicitly set the discriminator
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsEnvelope(env)
	assert.NoError(t, err)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	snap := p.InProgressSnapshots["foo"]
	assert.Len(t, snap.settings, 1)
	assert.Equal(t, "foo", snap.settings[0].Type)
}

func TestHandleSettingsEnvelope_OrphanEnvelope(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	env := stsSettingsModel.SettingsEnvelope{
		Id:      "unknown-snap",
		Setting: stsSettingsModel.Setting{Type: "foo"},
		Type:    "SettingsEnvelope", // explicitly set the discriminator
	}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsEnvelope(env)
	assert.NoError(t, err)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	// orphan envelopes are ignored
	_, exists := p.InProgressSnapshots["foo"]
	assert.False(t, exists)
}

func TestHandleSnapshotStop_UpdatesCache(t *testing.T) {
	p, cache := newProcessorWithMockCache(t)

	// Seed snapshot
	p.InProgressSnapshots["foo"] = &InProgressSnapshot{
		snapshotId:  "snap-1",
		settingType: "foo",
		settings: []stsSettingsModel.Setting{
			{Type: "foo"},
		},
	}

	stop := stsSettingsModel.SettingsSnapshotStop{Id: "snap-1", Type: "SettingsSnapshotStop"}
	cache.On("UpdateSettingsForType", stsSettingsModel.SettingType("foo"), mock.Anything).Once()

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStop(stop)
	assert.NoError(t, err)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	cache.AssertExpectations(t)

	// should be removed from the in progress snapshot cache
	_, exists := p.InProgressSnapshots["foo"]
	assert.False(t, exists)
}

func TestHandleSnapshotStop_OrphanStop(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	stop := stsSettingsModel.SettingsSnapshotStop{Id: "unknown", Type: "SettingsSnapshotStop"}

	settingsProtocol := stsSettingsModel.SettingsProtocol{}
	err := settingsProtocol.FromSettingsSnapshotStop(stop)
	assert.NoError(t, err)
	err = p.ProcessSettingsProtocol(&settingsProtocol)
	assert.NoError(t, err)

	assert.Empty(t, p.InProgressSnapshots)
}

func TestProcessSettingsProtocol_DiscriminatorError(t *testing.T) {
	p, _ := newProcessorWithMockCache(t)

	proto := &stsSettingsModel.SettingsProtocol{}

	err := p.ProcessSettingsProtocol(proto)
	assert.Error(t, err)
}

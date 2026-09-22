//nolint:testpackage
package topologyconnector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	stsSettingsAPI "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zaptest"
)

func metadataShutdownConnectors(t *testing.T, sink consumer.Logs, count int) (*MetadataPublisher, []*connectorImpl) {
	t.Helper()
	logger := zaptest.NewLogger(t)
	publisher := NewMetadataPublisher(logger)
	publisher.SetLogsConsumer(sink)
	signals := []settingsproto.OtelInputSignal{settingsproto.TRACES, settingsproto.METRICS, settingsproto.LOGS}
	manager := NewSnapshotManager(logger, signals, publisher)
	provider := NewMockStsSettingsProvider(nil, nil)
	host := &mockHost{ext: map[component.ID]component.Component{
		component.MustNewID(stsSettingsAPI.Type.String()): provider,
	}}
	connectors := make([]*connectorImpl, 0, count)
	for i := range count {
		conn := &connectorImpl{
			logger: logger, snapshotManager: manager, metadataPublisher: publisher, supportedSignal: signals[i%len(signals)],
		}
		require.NoError(t, conn.Start(t.Context(), host))
		connectors = append(connectors, conn)
	}
	return publisher, connectors
}

func queueMetadataForRemoval(publisher *MetadataPublisher) settingsproto.OtelComponentMapping {
	mapping := settingsproto.OtelComponentMapping{Identifier: "urn:shutdown:mapping", Name: "Mapping"}
	publisher.Update(nil, map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{
		settingsproto.TRACES: {mapping},
	}, nil)
	return mapping
}

func waitMetadataSender(t *testing.T, publisher *MetadataPublisher) {
	t.Helper()
	require.Eventually(t, func() bool {
		publisher.pendingMu.Lock()
		defer publisher.pendingMu.Unlock()
		return !publisher.publishing
	}, 5*time.Second, time.Millisecond)
}

func TestConnectorShutdown_DrainsTombstones(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	delivered := make(chan bool, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var closed atomic.Bool
	var calls atomic.Int32
	sink, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		for _, record := range extractMetadataRecords(t, logs) {
			if record.Body().Bytes().Len() == 0 {
				delivered <- !closed.Load()
			}
		}
		if closed.Load() {
			return errors.New("consumer is shut down")
		}
		return nil
	})
	require.NoError(t, err)
	publisher, connectors := metadataShutdownConnectors(t, sink, 3)
	t.Cleanup(func() {
		unblock()
		waitMetadataSender(t, publisher)
	})
	mapping := queueMetadataForRemoval(publisher)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata export did not start")
	}
	earlyCtx, earlyCancel := context.WithTimeout(t.Context(), time.Second)
	defer earlyCancel()
	notStarted := &connectorImpl{snapshotManager: connectors[0].snapshotManager, metadataPublisher: publisher}
	require.NoError(t, notStarted.Shutdown(earlyCtx))
	require.NoError(t, connectors[0].Shutdown(earlyCtx))
	require.NoError(t, connectors[0].Shutdown(earlyCtx))
	require.NoError(t, connectors[1].Shutdown(earlyCtx))
	publisher.PublishTombstones([]settingsproto.SettingExtension{mapping})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		err := connectors[2].Shutdown(ctx)
		closed.Store(true)
		finished <- err
	}()
	var shutdownErr error
	returned := false
	select {
	case shutdownErr = <-finished:
		returned = true
		t.Error("last connector shutdown returned with a pending tombstone")
	case <-time.After(100 * time.Millisecond):
	}
	read := make(chan struct{})
	go func() {
		connectors[2].snapshotManager.Current(settingsproto.TRACES)
		close(read)
	}()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Error("metadata draining held the snapshot lock")
	}
	unblock()
	if !returned {
		select {
		case shutdownErr = <-finished:
		case <-ctx.Done():
			t.Fatal("shutdown did not finish after the export was released")
		}
	}
	require.NoError(t, shutdownErr)
	select {
	case beforeClose := <-delivered:
		assert.True(t, beforeClose, "tombstone arrived after downstream shutdown")
	case <-time.After(time.Second):
		t.Fatal("queued tombstone was not exported")
	}
}

func TestConnectorShutdown_CancelsSenderAtDeadline(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	sink, err := consumer.NewLogs(func(ctx context.Context, _ plog.Logs) error {
		if calls.Add(1) == 1 {
			entered <- ctx
			<-release
		}
		return nil
	})
	require.NoError(t, err)
	publisher, connectors := metadataShutdownConnectors(t, sink, 1)
	t.Cleanup(func() {
		close(release)
		waitMetadataSender(t, publisher)
		assert.Equal(t, int32(1), calls.Load(), "pending tombstone was sent after shutdown timed out")
	})
	mapping := queueMetadataForRemoval(publisher)
	var exportCtx context.Context
	select {
	case exportCtx = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata export did not start")
	}
	publisher.PublishTombstones([]settingsproto.SettingExtension{mapping})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- connectors[0].Shutdown(ctx) }()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("shutdown ignored its deadline")
	}
	assert.ErrorIs(t, exportCtx.Err(), context.Canceled)
}

func TestConnectorShutdown_ReportsExportFailure(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	exportErr := errors.New("tombstone rejected")
	sink, err := consumer.NewLogs(func(context.Context, plog.Logs) error {
		close(entered)
		<-release
		return exportErr
	})
	require.NoError(t, err)
	publisher, connectors := metadataShutdownConnectors(t, sink, 1)
	t.Cleanup(func() {
		unblock()
		waitMetadataSender(t, publisher)
	})
	publisher.PublishTombstones([]settingsproto.SettingExtension{
		settingsproto.OtelComponentMapping{Identifier: "urn:shutdown:rejected"},
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata export did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- connectors[0].Shutdown(ctx) }()
	require.Eventually(t, func() bool {
		publisher.pendingMu.Lock()
		defer publisher.pendingMu.Unlock()
		return publisher.closing
	}, time.Second, time.Millisecond)
	unblock()
	select {
	case err := <-finished:
		assert.ErrorIs(t, err, exportErr)
	case <-ctx.Done():
		t.Fatal("shutdown did not finish after the export failed")
	}
}

func TestSnapshotManager_StopDeadlineDuringUpdate(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := NewMockStsSettingsProvider([]settingsproto.OtelComponentMapping{
		{Identifier: "urn:shutdown:removed", Input: settingsproto.OtelInput{Signal: []settingsproto.OtelInputSignal{settingsproto.TRACES}}},
	}, nil)
	manager := NewSnapshotManager(zaptest.NewLogger(t), []settingsproto.OtelInputSignal{settingsproto.TRACES})
	require.NoError(t, manager.Start(t.Context(), provider, func(
		context.Context, []settingsproto.OtelComponentMapping, []settingsproto.OtelRelationMapping,
	) {
		close(entered)
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		select {
		case <-manager.stopped:
		case <-time.After(5 * time.Second):
			t.Error("settings update loop did not stop")
		}
	})
	provider.ComponentMappings = nil
	provider.SettingUpdatesCh <- stsSettingsEvents.UpdateSettingsEvent{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("settings update did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := manager.Stop(ctx)
		finished <- err
	}()
	select {
	case err := <-finished:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("stopping settings updates ignored the shutdown deadline")
	}
}

type rejectingSettingsProvider struct {
	*MockStsSettingsProvider
}

func (*rejectingSettingsProvider) RegisterForUpdates(...settingsproto.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	return nil, errors.New("registration failed")
}

func TestSnapshotManager_FailedStartDoesNotRetainReference(t *testing.T) {
	manager := NewSnapshotManager(zaptest.NewLogger(t), []settingsproto.OtelInputSignal{settingsproto.TRACES})
	provider := NewMockStsSettingsProvider(nil, nil)
	require.ErrorContains(t, manager.Start(t.Context(), &rejectingSettingsProvider{provider}, nil), "registration failed")
	require.NoError(t, manager.Start(t.Context(), provider, nil))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	last, err := manager.Stop(ctx)
	require.NoError(t, err)
	assert.True(t, last, "failed registration retained a reference and prevented final shutdown")
}

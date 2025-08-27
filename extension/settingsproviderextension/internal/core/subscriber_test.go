package core

import (
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"testing"
	"time"
)

func newSubscriberHub(t *testing.T) *SubscriberHub {
	t.Helper()
	return NewSubscriberHub(zaptest.NewLogger(t))
}

func TestSubscriberHub_RegisterAddsSubscriber(t *testing.T) {
	h := newSubscriberHub(t)

	ch1, err := h.Register()
	require.NoError(t, err)
	ch2, err := h.Register()
	require.NoError(t, err)

	if h.SubscriptionCount() != 2 {
		t.Errorf("expected 2 subscription, got %d", h.SubscriptionCount())
	}

	// sanity check that channels are different
	if ch1 == ch2 {
		t.Errorf("expected different channels, got same")
	}
}

func TestSubscriberHub_NotifySendsSignal(t *testing.T) {
	h := newSubscriberHub(t)
	ch, err := h.Register() // no filter, receives all updates
	require.NoError(t, err)
	typ := stsSettingsModel.SettingTypeOtelComponentMapping

	event := stsSettingsEvents.UpdateSettingsEvent{}

	h.Notify(typ)

	select {
	case received := <-ch:
		if received != event {
			t.Errorf("expected event %+v, got %+v", event, received)
		}
	case <-time.After(50 * time.Millisecond):
		t.Errorf("expected notification, got none")
	}
}

func TestSubscriberHub_SubscriberReceivesOnlyMatchingTypes(t *testing.T) {
	h := newSubscriberHub(t)
	ch, err := h.Register(stsSettingsModel.SettingTypeOtelComponentMapping)
	require.NoError(t, err)
	matchingType := stsSettingsModel.SettingTypeOtelComponentMapping

	// Notify with a matching type
	matchingEvent := stsSettingsEvents.UpdateSettingsEvent{}
	h.Notify(matchingType)

	select {
	case got := <-ch:
		if got != matchingEvent {
			t.Errorf("expected %+v, got %+v", matchingEvent, got)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("expected matchingEvent event, got none")
	}

	// Notify with a non-matchingEvent type
	h.Notify(stsSettingsModel.SettingTypeOtelRelationMapping)

	select {
	case got := <-ch:
		t.Fatalf("expected no event, but got %+v", got)
	case <-time.After(50 * time.Millisecond):
		// success, no event delivered
	}
}

func TestSubscriberHub_SubscriberReceivesMultipleFilteredTypes(t *testing.T) {
	h := newSubscriberHub(t)
	settingTypes := []stsSettingsModel.SettingType{
		stsSettingsModel.SettingTypeOtelComponentMapping,
		stsSettingsModel.SettingTypeOtelRelationMapping,
	}

	ch, err := h.Register(settingTypes...)
	require.NoError(t, err)
	h.Notify(settingTypes...)

	received := []stsSettingsEvents.UpdateSettingsEvent{}
	for len(received) < len(settingTypes) {
		select {
		case got := <-ch:
			received = append(received, got)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected %d events, got %d", len(settingTypes), len(received))
		}
	}
}

func TestSubscriberHub_SubscriberFilterExcludesIrrelevantEvents(t *testing.T) {
	h := newSubscriberHub(t)
	ch, err := h.Register(stsSettingsModel.SettingTypeOtelRelationMapping)
	require.NoError(t, err)

	h.Notify(stsSettingsModel.SettingTypeOtelComponentMapping)

	select {
	case got := <-ch:
		t.Fatalf("expected no event, got %+v", got)
	case <-time.After(50 * time.Millisecond):
		// success, event ignored
	}
}

func TestSubscriberHub_MultipleSubscribersReceiveSignal(t *testing.T) {
	h := newSubscriberHub(t)
	ch1, err := h.Register()
	require.NoError(t, err)
	ch2, err := h.Register()
	require.NoError(t, err)

	h.Notify(stsSettingsModel.SettingTypeOtelComponentMapping)

	got1, got2 := false, false
	select {
	case <-ch1:
		got1 = true
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-ch2:
		got2 = true
	case <-time.After(50 * time.Millisecond):
	}

	if !got1 || !got2 {
		t.Errorf("expected both subscription to get signal, got1=%v got2=%v", got1, got2)
	}
}

// edge case
func TestSubscriberHub_NotifyWithNoSubscribersDoesNotPanic(t *testing.T) {
	h := newSubscriberHub(t)

	// Just make sure it doesn’t panic or deadlock
	h.Notify(stsSettingsModel.SettingTypeOtelComponentMapping)
}

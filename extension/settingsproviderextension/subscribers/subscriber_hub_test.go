package subscribers

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"go.uber.org/zap"
	"testing"
	"time"
)

func newSubscriberHub() *SubscriberHub {
	logger, _ := zap.NewDevelopment()
	return NewSubscriberHub(logger)
}

func TestSubscriberHub_RegisterAddsSubscriber(t *testing.T) {
	h := newSubscriberHub()

	ch1 := h.Register()
	ch2 := h.Register()

	if h.SubscriberCount() != 2 {
		t.Errorf("expected 2 subscribers, got %d", h.SubscriberCount())
	}

	// sanity check that channels are different
	if ch1 == ch2 {
		t.Errorf("expected different channels, got same")
	}
}

func TestSubscriberHub_NotifySendsSignal(t *testing.T) {
	h := newSubscriberHub()
	ch := h.Register() // no filter, receives all updates

	event := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}

	h.Notify(event)

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
	h := newSubscriberHub()
	ch := h.Register(stsSettingsModel.SettingTypeOtelComponentMapping)

	// Notify with a matching type
	matching := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}
	h.Notify(matching)

	select {
	case got := <-ch:
		if got != matching {
			t.Errorf("expected %+v, got %+v", matching, got)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("expected matching event, got none")
	}

	// Notify with a non-matching type
	nonMatching := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelRelationMapping,
	}
	h.Notify(nonMatching)

	select {
	case got := <-ch:
		t.Fatalf("expected no event, but got %+v", got)
	case <-time.After(50 * time.Millisecond):
		// success, no event delivered
	}
}

func TestSubscriberHub_SubscriberReceivesMultipleFilteredTypes(t *testing.T) {
	h := newSubscriberHub()
	ch := h.Register(
		stsSettingsModel.SettingTypeOtelComponentMapping,
		stsSettingsModel.SettingTypeOtelRelationMapping,
	)

	events := []stsSettingsEvents.UpdateSettingsEvent{
		{Type: stsSettingsModel.SettingTypeOtelComponentMapping},
		{Type: stsSettingsModel.SettingTypeOtelRelationMapping},
	}

	for _, e := range events {
		h.Notify(e)
	}

	received := []stsSettingsEvents.UpdateSettingsEvent{}
	for len(received) < len(events) {
		select {
		case got := <-ch:
			received = append(received, got)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected %d events, got %d", len(events), len(received))
		}
	}
}

func TestSubscriberHub_SubscriberFilterExcludesIrrelevantEvents(t *testing.T) {
	h := newSubscriberHub()
	ch := h.Register(stsSettingsModel.SettingTypeOtelRelationMapping)

	irrelevant := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}
	h.Notify(irrelevant)

	select {
	case got := <-ch:
		t.Fatalf("expected no event, got %+v", got)
	case <-time.After(50 * time.Millisecond):
		// success, event ignored
	}
}

func TestSubscriberHub_MultipleSubscribersReceiveSignal(t *testing.T) {
	h := newSubscriberHub()
	ch1 := h.Register()
	ch2 := h.Register()

	event := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}

	h.Notify(event)

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
		t.Errorf("expected both subscribers to get signal, got1=%v got2=%v", got1, got2)
	}
}

// edge case
func TestSubscriberHub_NotifyWithNoSubscribersDoesNotPanic(t *testing.T) {
	h := newSubscriberHub()

	event := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}

	// Just make sure it doesn’t panic or deadlock
	h.Notify(event)
}

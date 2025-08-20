package subscribers

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"testing"
	"time"
)

func TestSubscriberHub_RegisterAddsSubscriber(t *testing.T) {
	h := NewSubscriberHub()

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
	h := NewSubscriberHub()
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
	h := NewSubscriberHub()
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
	h := NewSubscriberHub()
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
	timeout := time.After(100 * time.Millisecond)
	for len(received) < len(events) {
		select {
		case got := <-ch:
			received = append(received, got)
		case <-timeout:
			t.Fatalf("expected %d events, got %d", len(events), len(received))
		}
	}
}

func TestSubscriberHub_SubscriberFilterExcludesIrrelevantEvents(t *testing.T) {
	h := NewSubscriberHub()
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

func TestSubscriberHub_NotifyDoesNotBlockOnUnconsumedSignal(t *testing.T) {
	h := NewSubscriberHub()
	ch := h.Register() // no filter, receives all updates

	event := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}

	// Fill the buffer with one pending signal (buffer size is 1).
	h.Notify(event)

	// Second notify should not block and should be skipped (since channel is full).
	done := make(chan struct{})
	go func() {
		h.Notify(event)
		close(done)
	}()

	select {
	case <-done:
		// should not block
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Notify blocked on a full channel")
	}

	// There should be one pending signal now. First non-blocking receive should succeed...
	select {
	case <-ch:
		// got one pending signal
	default:
		t.Fatalf("expected exactly one pending signal after skipped notify")
	}

	// ...and a second non-blocking receive should fail (no extra signal sent).
	select {
	case <-ch:
		t.Fatalf("unexpected extra signal: second notify should have been skipped")
	default:
		// no extra signal
	}
}

func TestSubscriberHub_MultipleSubscribersReceiveSignal(t *testing.T) {
	h := NewSubscriberHub()
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
	h := NewSubscriberHub()

	event := stsSettingsEvents.UpdateSettingsEvent{
		Type: stsSettingsModel.SettingTypeOtelComponentMapping,
	}

	// Just make sure it doesn’t panic or deadlock
	h.Notify(event)
}

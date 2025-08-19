package common

import (
	"testing"
	"time"
)

func TestRegisterAddsSubscriber(t *testing.T) {
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

func TestNotifySendsSignal(t *testing.T) {
	h := NewSubscriberHub()
	ch := h.Register()

	h.Notify()

	select {
	case <-ch:
		// success
	case <-time.After(50 * time.Millisecond):
		t.Errorf("expected notification, got none")
	}
}

func TestNotifyDoesNotBlockOnUnconsumedSignal(t *testing.T) {
	h := NewSubscriberHub()
	ch := h.Register()

	// Fill the buffer with one pending signal (buffer size is 1).
	h.Notify()

	// Second notify should not block and should be skipped (since ch is full).
	done := make(chan struct{})
	go func() {
		h.Notify()
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

func TestMultipleSubscribersReceiveSignal(t *testing.T) {
	h := NewSubscriberHub()
	ch1 := h.Register()
	ch2 := h.Register()

	h.Notify()

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

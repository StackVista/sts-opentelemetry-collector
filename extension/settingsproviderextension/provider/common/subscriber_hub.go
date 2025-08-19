package common

import "sync"

type SubscriberHub struct {
	// Mutex for concurrent access to subscribers
	subscribersLock sync.RWMutex
	// The list of subscribers to which signals that settings have been updated will be sent
	subscribers []chan struct{}
}

func NewSubscriberHub() *SubscriberHub {
	return &SubscriberHub{
		subscribers: make([]chan struct{}, 0),
	}
}

func (h *SubscriberHub) Register() <-chan struct{} {
	ch := make(chan struct{}, 1) // buffered to avoid blocking
	h.subscribersLock.Lock()
	h.subscribers = append(h.subscribers, ch)
	h.subscribersLock.Unlock()
	return ch
}

func (h *SubscriberHub) Notify() {
	h.subscribersLock.RLock()
	defer h.subscribersLock.RUnlock()
	for _, ch := range h.subscribers {
		select {
		case ch <- struct{}{}:
		default:
			// skip if subscriber hasn't drained previous signal
		}
	}
}

func (h *SubscriberHub) Unregister(ch <-chan struct{}) {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	for i, sub := range h.subscribers {
		if sub == ch {
			// Remove without preserving order
			h.subscribers[i] = h.subscribers[len(h.subscribers)-1]
			h.subscribers = h.subscribers[:len(h.subscribers)-1]
			close(sub) // close channel to signal shutdown
			break
		}
	}
}

func (h *SubscriberHub) SubscriberCount() int {
	h.subscribersLock.RLock()
	defer h.subscribersLock.RUnlock()
	return len(h.subscribers)
}

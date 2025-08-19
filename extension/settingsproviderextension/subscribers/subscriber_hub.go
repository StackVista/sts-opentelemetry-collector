package subscribers

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"sync"
)

type subscriber struct {
	settingTypes map[stsSettingsModel.SettingType]struct{}
	channel      chan stsSettingsEvents.UpdateSettingsEvent
}

type SubscriberHub struct {
	// Mutex for concurrent access to subscribers
	subscribersLock sync.RWMutex
	// The list of subscribers to which signals that settings have been updated will be sent
	subscribers []subscriber
}

func NewSubscriberHub() *SubscriberHub {
	return &SubscriberHub{
		subscribers: make([]subscriber, 0),
	}
}

func (h *SubscriberHub) Register(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	typeSet := make(map[stsSettingsModel.SettingType]struct{}, len(types))
	for _, t := range types {
		typeSet[t] = struct{}{}
	}

	// buffered so sender won’t block
	ch := make(chan stsSettingsEvents.UpdateSettingsEvent, len(types))
	h.subscribers = append(h.subscribers, subscriber{
		settingTypes: typeSet,
		channel:      ch,
	})
	return ch
}

func (h *SubscriberHub) Notify(event stsSettingsEvents.UpdateSettingsEvent) {
	h.subscribersLock.RLock()
	defer h.subscribersLock.RUnlock()

	for _, sub := range h.subscribers {
		// If subscriber registered with no filter
		if len(sub.settingTypes) == 0 {
			sub.channel <- event
			continue
		}
		if _, ok := sub.settingTypes[event.Type]; ok {
			sub.channel <- event
		}
	}
}

func (h *SubscriberHub) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	for i, sub := range h.subscribers {
		if sub.channel == ch {
			// Remove without preserving order
			h.subscribers[i] = h.subscribers[len(h.subscribers)-1]
			h.subscribers = h.subscribers[:len(h.subscribers)-1]
			close(sub.channel) // close channel to signal shutdown
			return true
		}
	}
	return false
}

func (h *SubscriberHub) SubscriberCount() int {
	h.subscribersLock.RLock()
	defer h.subscribersLock.RUnlock()
	return len(h.subscribers)
}

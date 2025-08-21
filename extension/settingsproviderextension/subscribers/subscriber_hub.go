package subscribers

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"go.uber.org/zap"
	"sync"
)

const (
	defaultBufferSize = 2
)

type subscriber struct {
	settingTypes map[stsSettingsModel.SettingType]struct{}
	channel      chan stsSettingsEvents.UpdateSettingsEvent
}

type SubscriberHub struct {
	logger *zap.Logger

	// Mutex for concurrent access to subscribers
	subscribersLock sync.RWMutex
	// The list of subscribers to which signals that settings have been updated will be sent
	subscribers []subscriber
}

func NewSubscriberHub(logger *zap.Logger) *SubscriberHub {
	return &SubscriberHub{
		logger:      logger,
		subscribers: make([]subscriber, 0),
	}
}

func (h *SubscriberHub) Register(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return h.RegisterWithBuffer(defaultBufferSize, types...)
}

func (h *SubscriberHub) RegisterWithBuffer(bufferSize int, types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	typeSet := make(map[stsSettingsModel.SettingType]struct{}, len(types))
	for _, t := range types {
		typeSet[t] = struct{}{}
	}

	// buffered so sender won’t block
	ch := make(chan stsSettingsEvents.UpdateSettingsEvent, bufferSize)
	h.subscribers = append(h.subscribers, subscriber{
		settingTypes: typeSet,
		channel:      ch,
	})
	return ch
}

func (h *SubscriberHub) Notify(types ...stsSettingsModel.SettingType) {
	go func() {
		h.subscribersLock.RLock()
		defer h.subscribersLock.RUnlock()

		for _, sub := range h.subscribers {
			// Subscriber with no filter: receives all types
			if len(sub.settingTypes) == 0 {
				for _, t := range types {
					h.nonBlockingSend(sub, stsSettingsEvents.UpdateSettingsEvent{Type: t})
				}
				continue
			}

			// Subscriber with filter: only receive matching types
			for _, t := range types {
				if _, ok := sub.settingTypes[t]; ok {
					h.nonBlockingSend(sub, stsSettingsEvents.UpdateSettingsEvent{Type: t})
				}
			}
		}
	}()
}

func (h *SubscriberHub) nonBlockingSend(sub subscriber, event stsSettingsEvents.UpdateSettingsEvent) {
	select {
	case sub.channel <- event:
		// Successfully sent
	default:
		// Subscriber buffer full, drop event (and wait for the next update)
		// TODO: have metrics for a slow subscriber?
		h.logger.Debug("Dropped settings event, subscriber buffer full", zap.String("settingType", string(event.Type)))
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

func (h *SubscriberHub) Shutdown() {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	for _, sub := range h.subscribers {
		close(sub.channel)
	}
	h.subscribers = nil
}

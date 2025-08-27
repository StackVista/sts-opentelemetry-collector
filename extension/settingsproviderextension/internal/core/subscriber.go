package core

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"go.uber.org/zap"
	"sync"
)

const (
	defaultBufferSize = 1
)

type Subscriber interface {
	Notify(types ...stsSettingsModel.SettingType)
	Register(types ...stsSettingsModel.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error)
	Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool
	Shutdown()
}

type subscription struct {
	settingTypes map[stsSettingsModel.SettingType]struct{}
	channel      chan stsSettingsEvents.UpdateSettingsEvent
}

type SubscriberHub struct {
	logger *zap.Logger

	// Mutex for concurrent access to subscriptions
	subscriptionsLock sync.RWMutex
	// The list of subscriptions to which signals that settings have been updated will be sent
	subscriptions []subscription
}

func NewSubscriberHub(logger *zap.Logger) *SubscriberHub {
	return &SubscriberHub{
		logger:        logger,
		subscriptions: make([]subscription, 0),
	}
}

func (h *SubscriberHub) Register(types ...stsSettingsModel.SettingType) (<-chan stsSettingsEvents.UpdateSettingsEvent, error) {
	h.subscriptionsLock.Lock()
	defer h.subscriptionsLock.Unlock()

	typeSet := make(map[stsSettingsModel.SettingType]struct{}, len(types))
	var missing []stsSettingsModel.SettingType
	for _, t := range types {
		if _, ok := ConverterFor(t); !ok {
			missing = append(missing, t)
		} else {
			typeSet[t] = struct{}{}
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("no converters registered for setting types: %v", missing)
	}

	// buffered so sender won’t block
	ch := make(chan stsSettingsEvents.UpdateSettingsEvent, defaultBufferSize)
	h.subscriptions = append(h.subscriptions, subscription{
		settingTypes: typeSet,
		channel:      ch,
	})

	return ch, nil
}

func (h *SubscriberHub) Notify(types ...stsSettingsModel.SettingType) {
	go func() {
		h.subscriptionsLock.RLock()
		defer h.subscriptionsLock.RUnlock()

		for _, sub := range h.subscriptions {
			// Subscriber with no filter: receives all types
			if len(sub.settingTypes) == 0 {
				for _, t := range types {
					h.nonBlockingSend(sub, t)
				}
				continue
			}

			// Subscriber with filter: only receive matching types
			for _, t := range types {
				if _, ok := sub.settingTypes[t]; ok {
					h.nonBlockingSend(sub, t)
				}
			}
		}
	}()
}

func (h *SubscriberHub) nonBlockingSend(sub subscription, settingType stsSettingsModel.SettingType) {
	select {
	case sub.channel <- stsSettingsEvents.UpdateSettingsEvent{}:
		// Successfully sent
	default:
		// Subscriber buffer full, drop event (and wait for the next update)
		// TODO: have metrics for a slow subscriber?
		h.logger.Debug("Dropped settings event, subscription buffer full", zap.String("settingType", string(settingType)))
	}
}

func (h *SubscriberHub) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	h.subscriptionsLock.Lock()
	defer h.subscriptionsLock.Unlock()

	for i, sub := range h.subscriptions {
		if sub.channel == ch {
			// Remove without preserving order
			h.subscriptions[i] = h.subscriptions[len(h.subscriptions)-1]
			h.subscriptions = h.subscriptions[:len(h.subscriptions)-1]
			close(sub.channel) // close channel to signal shutdown
			return true
		}
	}
	return false
}

func (h *SubscriberHub) SubscriptionCount() int {
	h.subscriptionsLock.RLock()
	defer h.subscriptionsLock.RUnlock()
	return len(h.subscriptions)
}

func (h *SubscriberHub) Shutdown() {
	h.subscriptionsLock.Lock()
	defer h.subscriptionsLock.Unlock()

	for _, sub := range h.subscriptions {
		close(sub.channel)
	}
	h.subscriptions = nil
}

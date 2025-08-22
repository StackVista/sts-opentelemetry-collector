package core

import (
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	"go.uber.org/zap"
	"sync"
)

const (
	defaultBufferSize = 2
)

type Subscriber interface {
	Notify(types ...stsSettingsModel.SettingType)
	Register(types ...stsSettingsModel.SettingType) <-chan UpdateSettingsEvent
	Unregister(ch <-chan UpdateSettingsEvent) bool
	Shutdown()
}

type UpdateSettingsEvent struct{}

type subscription struct {
	settingTypes map[stsSettingsModel.SettingType]struct{}
	channel      chan UpdateSettingsEvent
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

func (h *SubscriberHub) Register(types ...stsSettingsModel.SettingType) <-chan UpdateSettingsEvent {
	return h.RegisterWithBuffer(defaultBufferSize, types...)
}

func (h *SubscriberHub) RegisterWithBuffer(bufferSize int, types ...stsSettingsModel.SettingType) <-chan UpdateSettingsEvent {
	h.subscriptionsLock.Lock()
	defer h.subscriptionsLock.Unlock()

	typeSet := make(map[stsSettingsModel.SettingType]struct{}, len(types))
	for _, t := range types {
		typeSet[t] = struct{}{}
	}

	// buffered so sender won’t block
	ch := make(chan UpdateSettingsEvent, bufferSize)
	h.subscriptions = append(h.subscriptions, subscription{
		settingTypes: typeSet,
		channel:      ch,
	})
	return ch
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
	case sub.channel <- UpdateSettingsEvent{}:
		// Successfully sent
	default:
		// Subscriber buffer full, drop event (and wait for the next update)
		// TODO: have metrics for a slow subscriber?
		h.logger.Debug("Dropped settings event, subscription buffer full", zap.String("settingType", string(settingType)))
	}
}

func (h *SubscriberHub) Unregister(ch <-chan UpdateSettingsEvent) bool {
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

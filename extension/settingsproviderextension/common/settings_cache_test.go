package common

import (
	"fmt"
	stsSettingsModel "github.com/stackvista/sts-opentelemetry-collector/connector/tracetotopoconnector/generated/settings"
	stsSettingsEvents "github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/events"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/subscribers"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"sort"
	"testing"
)

func newSettingsCache(subscriptionService subscribers.SubscriptionService) *DefaultSettingsCache {
	logger := zap.NewNop()

	return &DefaultSettingsCache{
		logger:              logger,
		settingsByType:      make(SettingsByType),
		subscriptionService: subscriptionService,
	}
}

type mockSubscriberHub struct {
	notified []stsSettingsModel.SettingType
}

func (m *mockSubscriberHub) Notify(settingTypes ...stsSettingsModel.SettingType) {
	m.notified = append(m.notified, settingTypes...)
}
func (m *mockSubscriberHub) Register(types ...stsSettingsModel.SettingType) <-chan stsSettingsEvents.UpdateSettingsEvent {
	return nil
}
func (m *mockSubscriberHub) Unregister(ch <-chan stsSettingsEvents.UpdateSettingsEvent) bool {
	return false
}

func (m *mockSubscriberHub) Shutdown() {}

func TestSettingsCache_GetAvailableSettingTypes(t *testing.T) {
	cache := newSettingsCache(&mockSubscriberHub{})
	require.Empty(t, cache.GetAvailableSettingTypes())

	cache.UpdateSettingsForType("bar", []SettingEntry{})
	types := cache.GetAvailableSettingTypes()

	require.Contains(t, types, stsSettingsModel.SettingType("bar"))
}

func TestSettingsCache_GetConcreteSettingsByType(t *testing.T) {
	cache := newSettingsCache(&mockSubscriberHub{})

	// custom type with converter
	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	cache.UpdateSettingsForType(typeA, []SettingEntry{entry})

	// first call returns concrete (converted) type
	vals, err := cache.GetConcreteSettingsByType(typeA)
	require.NoError(t, err)
	require.Equal(t, []any{"A"}, vals)

	// second call should reuse same concrete (converted) type
	vals2, err := cache.GetConcreteSettingsByType(typeA)
	require.NoError(t, err)
	require.Equal(t, vals, vals2)
}

func TestSettingsCache_UpdateSettingsForType_InitialUpdateTriggersNotification(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// custom type with converter
	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	cache.UpdateSettingsForType(typeA, []SettingEntry{entry})

	if len(subscriptionService.notified) != 1 || subscriptionService.notified[0] != typeA {
		t.Errorf("expected notification for first update")
	}
}

func TestSettingsCache_UpdateSettingsForType_WithSameValueDoesNotNotify(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// custom type with converter
	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	// seed cache
	cache.UpdateSettingsForType(typeA, []SettingEntry{entry})

	subscriptionService.notified = nil

	// update using the same value
	cache.UpdateSettingsForType(typeA, []SettingEntry{entry})
	if len(subscriptionService.notified) > 0 {
		t.Errorf("expected no notification for unchanged entries")
	}
}

func TestSettingsCache_UpdateSettingsForType_WithDifferentValueTriggersNotification(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// custom type with converter
	typeA := stsSettingsModel.SettingType("A")
	entryA := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	// seed cache
	cache.UpdateSettingsForType(typeA, []SettingEntry{entryA})

	subscriptionService.notified = nil

	typeB := stsSettingsModel.SettingType("A")
	entryB := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeB)})
	registerConverter(typeB, func(s stsSettingsModel.Setting) (any, error) { return "B", nil })

	// update using a different value
	cache.UpdateSettingsForType(typeB, []SettingEntry{entryB})

	if len(subscriptionService.notified) != 1 || subscriptionService.notified[0] != typeB {
		t.Errorf("expected notification for changed entries")
	}
}

// edge case - a conversion failing means there's no converter registered for the type, or the setting is malformed
func TestSettingsCache_UpdateSettingsForType_ConverterErrorDoesNotTriggerNotification(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// custom type with converter
	errorType := stsSettingsModel.SettingType("Error")
	errorEntry := NewSettingEntry(stsSettingsModel.Setting{Type: string(errorType)})
	registerConverter(errorType, func(s stsSettingsModel.Setting) (any, error) { return nil, fmt.Errorf("conversion failed") })

	cache.UpdateSettingsForType(errorType, []SettingEntry{errorEntry})

	if len(subscriptionService.notified) > 0 {
		t.Errorf("no notification expected when converter fails")
	}
}

func TestSettingsCache_Update_AddNewTypesTriggersNotifications(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// custom type with converter
	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	cache.Update(SettingsByType{
		typeA: {entry},
	})

	if len(subscriptionService.notified) != 1 || subscriptionService.notified[0] != typeA {
		t.Errorf("expected notification for added type")
	}
}

func TestSettingsCache_Update_UpdateExistingTypeWithChangeTriggersNotification(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	cache.Update(SettingsByType{
		typeA: {entry},
	})

	subscriptionService.notified = nil

	// before sending the same type, we register a converter that will change the value of the concrete type
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A-changed", nil })
	cache.Update(SettingsByType{
		typeA: {NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})},
	})

	if len(subscriptionService.notified) != 1 || subscriptionService.notified[0] != typeA {
		t.Errorf("expected notification for changed type")
	}
}

func TestSettingsCache_Update_DeletedTypeTriggersNotification(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	typeA := stsSettingsModel.SettingType("A")
	entry := NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})
	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })

	cache.Update(SettingsByType{
		typeA: {entry},
	})

	subscriptionService.notified = nil

	// Pass empty snapshot, which deletes existing type (A)
	cache.Update(SettingsByType{})

	if len(subscriptionService.notified) != 1 || subscriptionService.notified[0] != typeA {
		t.Errorf("expected notification for deleted type")
	}
}

func TestSettingsCache_Update_MixedAddUpdateDeleteTriggersAllNotifications(t *testing.T) {
	subscriptionService := &mockSubscriberHub{}
	cache := newSettingsCache(subscriptionService)

	// Prepare multiple types
	typeA := stsSettingsModel.SettingType("A")
	typeB := stsSettingsModel.SettingType("B")
	typeC := stsSettingsModel.SettingType("C")

	registerConverter(typeA, func(s stsSettingsModel.Setting) (any, error) { return "A", nil })
	registerConverter(typeB, func(s stsSettingsModel.Setting) (any, error) { return "B", nil })
	registerConverter(typeC, func(s stsSettingsModel.Setting) (any, error) { return "C", nil })

	// Seed cache with typeB and typeC
	cache.Update(SettingsByType{
		typeB: {NewSettingEntry(stsSettingsModel.Setting{Type: string(typeB)})},
		typeC: {NewSettingEntry(stsSettingsModel.Setting{Type: string(typeC)})},
	})

	subscriptionService.notified = nil

	// Now provide a new snapshot: add typeA, update typeB, delete typeC
	registerConverter(typeB, func(s stsSettingsModel.Setting) (any, error) { return "B-changed", nil })
	cache.Update(SettingsByType{
		typeA: {NewSettingEntry(stsSettingsModel.Setting{Type: string(typeA)})},
		typeB: {NewSettingEntry(stsSettingsModel.Setting{Type: string(typeB)})},
	})

	expected := []stsSettingsModel.SettingType{typeA, typeB, typeC}
	if !equalSlicesSorted(subscriptionService.notified, expected) {
		t.Errorf("expected notifications for all changes: got %v", subscriptionService.notified)
	}
}

func equalSlicesSorted(a, b []stsSettingsModel.SettingType) bool {
	if len(a) != len(b) {
		return false
	}

	// Sort both slices in place
	sort.Slice(a, func(i, j int) bool { return string(a[i]) < string(a[j]) })
	sort.Slice(b, func(i, j int) bool { return string(b[i]) < string(b[j]) })

	// Compare element by element
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

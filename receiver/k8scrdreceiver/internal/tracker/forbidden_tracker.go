package tracker

import (
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/emit"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ForbiddenTracker tracks resources with permission denied to avoid repeated attempts.
type ForbiddenTracker struct {
	mu            sync.RWMutex
	forbidden     map[string]time.Time // key: GVR string, value: last attempt time
	retryInterval time.Duration
}

// NewForbiddenTracker creates a new tracker with the given retry interval.
func NewForbiddenTracker(retryInterval time.Duration) *ForbiddenTracker {
	return &ForbiddenTracker{
		forbidden:     make(map[string]time.Time),
		retryInterval: retryInterval,
	}
}

// ShouldRetry returns true if the resource should be retried (not forbidden or past retry interval).
// Returns (shouldRetry bool, retryIn duration). If shouldRetry is false, retryIn indicates time until next retry.
func (f *ForbiddenTracker) ShouldRetry(gvr schema.GroupVersionResource) (bool, time.Duration) {
	key := emit.FormatGVRKey(gvr)

	f.mu.RLock()
	lastAttempt, isForbidden := f.forbidden[key]
	f.mu.RUnlock()

	if !isForbidden {
		return true, 0
	}

	elapsed := time.Since(lastAttempt)
	if elapsed >= f.retryInterval {
		return true, 0
	}

	return false, f.retryInterval - elapsed
}

// MarkForbidden marks a resource as forbidden with the current timestamp.
func (f *ForbiddenTracker) MarkForbidden(gvr schema.GroupVersionResource) {
	key := emit.FormatGVRKey(gvr)

	f.mu.Lock()
	f.forbidden[key] = time.Now()
	f.mu.Unlock()
}

// Clear removes a resource from the forbidden list (e.g., when CRD is deleted).
func (f *ForbiddenTracker) Clear(gvr schema.GroupVersionResource) {
	key := emit.FormatGVRKey(gvr)

	f.mu.Lock()
	delete(f.forbidden, key)
	f.mu.Unlock()
}

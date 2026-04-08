package k8scrdreceiver

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// forbiddenTracker tracks resources with permission denied to avoid repeated attempts
type forbiddenTracker struct {
	mu            sync.RWMutex
	forbidden     map[string]time.Time // key: GVR string, value: last attempt time
	retryInterval time.Duration
}

// newForbiddenTracker creates a new tracker with the given retry interval
func newForbiddenTracker(retryInterval time.Duration) *forbiddenTracker {
	return &forbiddenTracker{
		forbidden:     make(map[string]time.Time),
		retryInterval: retryInterval,
	}
}

// shouldRetry returns true if the resource should be retried (not forbidden or past retry interval)
// Returns (shouldRetry bool, retryIn duration). If shouldRetry is false, retryIn indicates time until next retry.
func (f *forbiddenTracker) shouldRetry(gvr schema.GroupVersionResource) (bool, time.Duration) {
	key := formatGVRKey(gvr)

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

// markForbidden marks a resource as forbidden with the current timestamp
func (f *forbiddenTracker) markForbidden(gvr schema.GroupVersionResource) {
	key := formatGVRKey(gvr)

	f.mu.Lock()
	f.forbidden[key] = time.Now()
	f.mu.Unlock()
}

// clear removes a resource from the forbidden list (e.g., when CRD is deleted)
func (f *forbiddenTracker) clear(gvr schema.GroupVersionResource) {
	key := formatGVRKey(gvr)

	f.mu.Lock()
	delete(f.forbidden, key)
	f.mu.Unlock()
}

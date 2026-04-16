package tracker_test

import (
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8scrdreceiver/internal/tracker"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestForbiddenTracker_ShouldRetry(t *testing.T) {
	tests := []struct {
		name            string
		retryInterval   time.Duration
		markForbidden   bool
		waitDuration    time.Duration
		wantShouldRetry bool
		wantRetryInGT   time.Duration // Greater than
	}{
		{
			name:            "not forbidden - should retry",
			retryInterval:   1 * time.Hour,
			markForbidden:   false,
			waitDuration:    0,
			wantShouldRetry: true,
			wantRetryInGT:   0,
		},
		{
			name:            "forbidden recently - should not retry",
			retryInterval:   1 * time.Hour,
			markForbidden:   true,
			waitDuration:    0,
			wantShouldRetry: false,
			wantRetryInGT:   59 * time.Minute, // Should be close to 1 hour
		},
		{
			name:            "forbidden past retry interval - should retry",
			retryInterval:   10 * time.Millisecond,
			markForbidden:   true,
			waitDuration:    15 * time.Millisecond,
			wantShouldRetry: true,
			wantRetryInGT:   0,
		},
		{
			name:            "forbidden at boundary - should retry",
			retryInterval:   10 * time.Millisecond,
			markForbidden:   true,
			waitDuration:    10 * time.Millisecond,
			wantShouldRetry: true,
			wantRetryInGT:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := tracker.NewForbiddenTracker(tt.retryInterval)
			gvr := schema.GroupVersionResource{
				Group:    "policies.kubewarden.io",
				Version:  "v1",
				Resource: "policyservers",
			}

			if tt.markForbidden {
				ft.MarkForbidden(gvr)
				if tt.waitDuration > 0 {
					time.Sleep(tt.waitDuration)
				}
			}

			shouldRetry, retryIn := ft.ShouldRetry(gvr)

			assert.Equal(t, tt.wantShouldRetry, shouldRetry)
			if !shouldRetry {
				assert.Greater(t, retryIn, tt.wantRetryInGT,
					"retryIn should be greater than %v, got %v", tt.wantRetryInGT, retryIn)
			} else {
				assert.Equal(t, time.Duration(0), retryIn)
			}
		})
	}
}

func TestForbiddenTracker_MarkForbidden(t *testing.T) {
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	gvr := schema.GroupVersionResource{
		Group:    "longhorn.io",
		Version:  "v1beta1",
		Resource: "volumes",
	}

	// Initially not forbidden
	shouldRetry, _ := ft.ShouldRetry(gvr)
	assert.True(t, shouldRetry)

	// Mark as forbidden
	ft.MarkForbidden(gvr)

	// Now should not retry
	shouldRetry, retryIn := ft.ShouldRetry(gvr)
	assert.False(t, shouldRetry)
	assert.Greater(t, retryIn, time.Duration(0))
}

func TestForbiddenTracker_Clear(t *testing.T) {
	ft := tracker.NewForbiddenTracker(1 * time.Hour)
	gvr := schema.GroupVersionResource{
		Group:    "example.com",
		Version:  "v1",
		Resource: "foos",
	}

	// Mark as forbidden
	ft.MarkForbidden(gvr)
	shouldRetry, _ := ft.ShouldRetry(gvr)
	assert.False(t, shouldRetry, "should not retry when forbidden")

	// Clear forbidden status
	ft.Clear(gvr)

	// Should retry after clear
	shouldRetry, retryIn := ft.ShouldRetry(gvr)
	assert.True(t, shouldRetry, "should retry after clear")
	assert.Equal(t, time.Duration(0), retryIn)
}

func TestForbiddenTracker_MultipleResources(t *testing.T) {
	ft := tracker.NewForbiddenTracker(1 * time.Hour)

	gvr1 := schema.GroupVersionResource{Group: "group1.io", Version: "v1", Resource: "res1"}
	gvr2 := schema.GroupVersionResource{Group: "group2.io", Version: "v1", Resource: "res2"}

	// Mark only gvr1 as forbidden
	ft.MarkForbidden(gvr1)

	// gvr1 should not retry
	shouldRetry1, _ := ft.ShouldRetry(gvr1)
	assert.False(t, shouldRetry1)

	// gvr2 should still retry
	shouldRetry2, _ := ft.ShouldRetry(gvr2)
	assert.True(t, shouldRetry2)

	// Clear gvr1
	ft.Clear(gvr1)

	// Both should retry now
	shouldRetry1, _ = ft.ShouldRetry(gvr1)
	shouldRetry2, _ = ft.ShouldRetry(gvr2)
	assert.True(t, shouldRetry1)
	assert.True(t, shouldRetry2)
}

func TestForbiddenTracker_Concurrency(_ *testing.T) {
	ft := tracker.NewForbiddenTracker(1 * time.Millisecond)
	gvr := schema.GroupVersionResource{Group: "test.io", Version: "v1", Resource: "test"}

	// Run concurrent mark/check/clear operations
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				ft.MarkForbidden(gvr)
				ft.ShouldRetry(gvr)
				ft.Clear(gvr)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// No race detector errors means test passes
}

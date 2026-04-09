package k8scrdreceiver

import "context"

// mode represents a collection strategy (pull or watch).
// Each mode runs independently and is managed by the receiver orchestrator.
type mode interface {
	// Start begins the collection mode. The context is the receiver's lifecycle context.
	Start(ctx context.Context) error
	// Shutdown gracefully stops the collection mode.
	Shutdown(ctx context.Context) error
}

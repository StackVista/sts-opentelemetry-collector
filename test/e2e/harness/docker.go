package harness

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"testing"
)

// EnsureNetwork creates a Docker network for e2e tests.
// The network is automatically removed when the test finishes.
func EnsureNetwork(ctx context.Context, t *testing.T) *testcontainers.DockerNetwork {
	t.Helper()

	logger := zaptest.NewLogger(t)

	nw, err := network.New(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nw.Remove(ctx)
		logger.Info("Docker network removed", zap.String("networkName", nw.Name))
	})

	return nw
}

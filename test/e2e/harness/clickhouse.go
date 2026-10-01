package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// Keep in step with clickhouse.image.tag in the suse-observability chart.
const defaultClickHouseImage = "quay.io/stackstate/clickhouse:26.8.2.7-so3"

const (
	ClickHouseUser     = "admin"
	ClickHousePassword = "admin"
)

type ClickHouseInstance struct {
	Container testcontainers.Container
	// NativeAddr is the native protocol address reachable from other containers on the test network.
	NativeAddr string
	httpURL    string
}

func clickHouseImage() string {
	if img := os.Getenv("CLICKHOUSE_IMAGE"); img != "" {
		return img
	}
	return defaultClickHouseImage
}

// StartClickHouse starts a single-node ClickHouse on the given network.
// The container is automatically terminated when the test finishes.
func StartClickHouse(ctx context.Context, t *testing.T, networkName string) *ClickHouseInstance {
	t.Helper()
	logger := zaptest.NewLogger(t)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        clickHouseImage(),
			ExposedPorts: []string{"8123/tcp"},
			Env: map[string]string{
				"CLICKHOUSE_USER":                      ClickHouseUser,
				"CLICKHOUSE_PASSWORD":                  ClickHousePassword,
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
			},
			Networks:   []string{networkName},
			WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)

	containerJSON, err := container.Inspect(ctx)
	require.NoError(t, err)
	containerName := strings.TrimPrefix(containerJSON.Name, "/")

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
		logger.Info("ClickHouse (testcontainer) terminated", zap.String("containerName", containerName))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	httpPort, err := container.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	logger.Info("ClickHouse (testcontainer) started",
		zap.String("network", networkName), zap.String("containerName", containerName))

	return &ClickHouseInstance{
		Container:  container,
		NativeAddr: fmt.Sprintf("%s:9000", containerName),
		httpURL:    fmt.Sprintf("http://%s:%s/", host, httpPort.Port()),
	}
}

// Query runs a query over the HTTP interface and decodes the JSONEachRow output.
func (ch *ClickHouseInstance) Query(ctx context.Context, query string) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		ch.httpURL+"?"+url.Values{"default_format": {"JSONEachRow"}}.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(ClickHouseUser, ClickHousePassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("clickhouse query failed (%d): %s", resp.StatusCode, body)
	}

	var rows []map[string]any
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

// QueryUntil polls the query until done accepts the rows or the timeout expires.
func (ch *ClickHouseInstance) QueryUntil(
	ctx context.Context, t *testing.T, query string, timeout time.Duration, done func([]map[string]any) bool,
) []map[string]any {
	t.Helper()
	var rows []map[string]any
	require.Eventually(t, func() bool {
		var err error
		rows, err = ch.Query(ctx, query)
		return err == nil && done(rows)
	}, timeout, 500*time.Millisecond, "query did not reach the expected state: %s (last result: %v)", query, rows)
	return rows
}

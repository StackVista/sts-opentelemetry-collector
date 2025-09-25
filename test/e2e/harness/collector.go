package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

const defaultCollectorImage = "sts-opentelemetry-collector:latest" // local default

//nolint:gochecknoglobals
var defaultCollectorTemplate string

func init() {
	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	baseDir := filepath.Dir(filename)
	defaultCollectorTemplate = filepath.Join(baseDir, "../templates/collector-config.yaml.tmpl")
}

type CollectorInstance struct {
	Container  testcontainers.Container
	InstanceID int
	HostAddr   string // discovered mapped port
}

type CollectorConfig struct {
	NumCollectors     int
	TemplatePath      string
	DockerNetworkName string
	KafkaBroker       string
	SettingsTopic     string
	TopologyTopic     string
	OtlpGRPCEndpoint  string
}

func collectorImage() string {
	if img := os.Getenv("OTEL_COLLECTOR_IMAGE"); img != "" {
		return img
	}
	return defaultCollectorImage
}

// BuildCollectorImage supports rebuilding the image after source code (collector) changes. Individual tests don't need
// to invoke this function - it's supposed to be called once, like in main_test.go.
// Example: REBUILD_COLLECTOR_IMAGE=1 go test -v ./test/e2e/...
func BuildCollectorImage() error {
	if os.Getenv("REBUILD_COLLECTOR_IMAGE") != "1" {
		return nil
	}

	//nolint:dogsled
	_, filename, _, _ := runtime.Caller(0)
	baseDir := filepath.Join(filepath.Dir(filename), "../../..")

	cmd := exec.Command("docker", "build", "-t", defaultCollectorImage, baseDir)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	logf := func(line string) {
		fmt.Fprintf(os.Stdout, "[docker build] %s\n", line)
		_ = os.Stderr.Sync() // force flush
	}

	go streamLines(stdout, logf)
	go streamLines(stderr, logf)

	return cmd.Wait()
}

func streamLines(r io.Reader, logf func(string)) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		logf(sc.Text())
	}
}

func WriteCollectorConfig(cfg CollectorConfig, instanceID int) (string, error) {
	templatePath := cfg.TemplatePath
	if templatePath == "" {
		templatePath = defaultCollectorTemplate
	}

	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		return "", err
	}

	outPath := filepath.Join(os.TempDir(), fmt.Sprintf("collector-instance-%d.yaml", instanceID))
	f, err := os.Create(outPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := tmpl.Execute(f, cfg); err != nil {
		return "", err
	}
	return outPath, nil
}

// StartCollector starts an otel collector instance.
// The container is automatically terminated when the test finishes.
func StartCollector(
	ctx context.Context,
	t *testing.T,
	configFile string,
	networkName string,
	instanceID int,
) *CollectorInstance {
	t.Helper()

	logger := zaptest.NewLogger(t)
	collectorImg := collectorImage()
	logger.Info("Using OpenTelemetry Collector image", zap.String("image", collectorImg))

	req := testcontainers.ContainerRequest{
		Image:        collectorImg,
		ExposedPorts: []string{"4317/tcp"}, // container-side OTLP gRPC
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      configFile,
				ContainerFilePath: "/etc/otelcol/config.yaml",
				FileMode:          0644,
			},
		},
		Cmd:        []string{"--config", "/etc/otelcol/config.yaml"},
		Networks:   []string{networkName},
		WaitingFor: wait.ForLog("Everything is ready"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	containerJSON, err := container.Inspect(ctx)
	require.NoError(t, err)
	containerName := strings.TrimPrefix(containerJSON.Name, "/")

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
		logger.Info("Otel collector (testcontainer) terminated", zap.String("containerName", containerName))
	})

	// Discover the mapped host port for OTLP
	mappedPort, err := container.MappedPort(ctx, "4317/tcp")
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	logger.Info(
		"Otel collector instance (testcontainer) started",
		zap.String("network", networkName),
		zap.String("host", host),
		zap.String("mappedPort", mappedPort.Port()),
		zap.String("containerName", containerName),
	)

	return &CollectorInstance{
		Container:  container,
		InstanceID: instanceID,
		HostAddr:   fmt.Sprintf("%s:%s", host, mappedPort.Port()),
	}
}

func StartCollectors(ctx context.Context, t *testing.T, providedConfig CollectorConfig) []*CollectorInstance {
	var collectors []*CollectorInstance

	for idx := 0; idx < providedConfig.NumCollectors; idx++ {
		collectorCfg := providedConfig

		// inside the container we always bind to 0.0.0.0:<port>
		port := 4317 + idx
		collectorCfg.OtlpGRPCEndpoint = fmt.Sprintf("0.0.0.0:%d", port)

		configFile, err := WriteCollectorConfig(collectorCfg, idx)
		require.NoError(t, err)

		inst := StartCollector(ctx, t, configFile, providedConfig.DockerNetworkName, idx)
		require.NoError(t, err)
		collectors = append(collectors, inst)
	}

	return collectors
}

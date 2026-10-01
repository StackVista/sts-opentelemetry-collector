package stsk8stopologyexporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/log"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates the cluster-agent-compatible Kubernetes topology exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType("stsk8stopology"),
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings:       exporterhelper.TimeoutConfig{Timeout: 30 * time.Second},
		ClusterType:           clusterTypeKubernetes,
		Interval:              90 * time.Second,
		SnapshotMaxAge:        15 * time.Minute,
		CollectTimeout:        10 * time.Minute,
		MaxElementsPerRequest: 10000,
		MaxAttempts:           3,
		ConfigMapMaxDataSize:  100 * 1024,
		Resources: ResourcesConfig{
			PersistentVolumes:      true,
			PersistentVolumeClaims: true,
			Namespaces:             true,
			ConfigMaps:             true,
			Secrets:                true,
			DaemonSets:             true,
			Deployments:            true,
			ReplicaSets:            true,
			StatefulSets:           true,
			Ingresses:              true,
			Jobs:                   true,
			CronJobs:               true,
		},
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid Kubernetes topology exporter configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	producer := cfg.InternalHostname
	if producer == "" {
		podHostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("internal_hostname is not set and the hostname is unavailable: %w", err)
		}
		producer = podHostname
	}
	userAgent := set.BuildInfo.Command + "/" + set.BuildInfo.Version
	api, _, err := openapiclient.NewOpenAPIClientWithOptions(ctx, openapiclient.ConnectionOptions{
		ReceiverURL:        strings.TrimSuffix(cfg.Endpoint, "/intake"),
		APIKey:             string(cfg.APIKey),
		ProxyURL:           string(cfg.ProxyURL),
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		RequestTimeout:     cfg.TimeoutSettings.Timeout,
		UserAgent:          userAgent,
	})
	if err != nil {
		return nil, err
	}
	connection := api.GetConfig()
	sender := &intakeSender{
		client:      connection.HTTPClient,
		endpoint:    connection.Servers[0].URL + intakeSuffix,
		apiKey:      string(cfg.APIKey),
		userAgent:   userAgent,
		maxAttempts: cfg.MaxAttempts,
		backoff:     defaultBackoff,
	}

	log.SetLogger(set.Logger)
	exp := newTopologyExporter(cfg, set.Logger, sender)
	exp.producer = producer
	return exporterhelper.NewLogs(ctx, set, cfg, exp.consumeLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(configretry.BackOffConfig{Enabled: false}),
		// Records must be applied in emission order for snapshot boundaries to hold.
		exporterhelper.WithQueue(configoptional.None[exporterhelper.QueueBatchConfig]()),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

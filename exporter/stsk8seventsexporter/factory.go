package stsk8seventsexporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/eventmapper"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/urn"
)

// NewFactory creates the cluster-agent-compatible Kubernetes events exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType("stsk8sevents"),
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings:     exporterhelper.TimeoutConfig{Timeout: 30 * time.Second},
		ClusterType:         clusterTypeKubernetes,
		Interval:            15 * time.Second,
		MaxEventsPerRequest: 1000,
		MaxBufferedEvents:   10000,
		MaxAttempts:         3,
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid Kubernetes events exporter configuration")
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
	clusterType := urn.ClusterTypeFromString(cfg.ClusterType)
	mapper := eventmapper.New(clusterType, cfg.ClusterName, cfg.categoryOverrides(), set.Logger)
	exp := newEventsExporter(cfg, set.Logger, sender, mapper, producer)
	return exporterhelper.NewLogs(ctx, set, cfg, exp.consumeLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(configretry.BackOffConfig{Enabled: false}),
		exporterhelper.WithQueue(configoptional.None[exporterhelper.QueueBatchConfig]()),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

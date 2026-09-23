package stsk8slogsexporter

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/openapiclient"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates the Promtail-compatible Kubernetes logs exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType("stsk8slogs"),
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	retry := configretry.NewDefaultBackOffConfig()
	retry.InitialInterval = time.Second
	retry.MaxInterval = 5 * time.Second
	retry.MaxElapsedTime = 30 * time.Second
	return &Config{
		TimeoutSettings: exporterhelper.TimeoutConfig{Timeout: 5 * time.Second},
		BackOffConfig:   retry,
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid Promtail-compatible logs exporter configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts := openapiclient.ConnectionOptions{
		ReceiverURL:        strings.TrimSuffix(cfg.Endpoint, "/logs/k8s"),
		APIKey:             string(cfg.APIKey),
		ProxyURL:           string(cfg.ProxyURL),
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		RequestTimeout:     cfg.TimeoutSettings.Timeout,
		UserAgent:          set.BuildInfo.Command + "/" + set.BuildInfo.Version,
	}
	if cfg.TLS.CAFile != "" {
		pem, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, errors.New("cannot read Promtail-compatible logs CA bundle")
		}
		if len(pem) == 0 {
			return nil, errors.New("the Promtail-compatible logs CA bundle is empty")
		}
		opts.CABundlePEM = pem
	}
	api, _, err := openapiclient.NewOpenAPIClientWithOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	connection := api.GetConfig()
	endpoint := connection.Servers[0].URL + "/stsAgent/logs/k8s"
	sender, err := newSender(connection.HTTPClient, endpoint, string(cfg.APIKey), cfg.TimeoutSettings.Timeout)
	if err != nil {
		return nil, err
	}
	return newLogsExporter(ctx, set, cfg, sender)
}

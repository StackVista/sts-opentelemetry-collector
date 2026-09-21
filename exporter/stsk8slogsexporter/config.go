package stsk8slogsexporter

import (
	"errors"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config configures the legacy Receiver endpoint and bounded in-memory delivery.
type Config struct {
	Endpoint        string                                                   `mapstructure:"endpoint"`
	APIKey          configopaque.String                                      `mapstructure:"api_key"`
	ClusterName     string                                                   `mapstructure:"cluster_name"`
	ProxyURL        configopaque.String                                      `mapstructure:"proxy_url"`
	TLS             TLSConfig                                                `mapstructure:"tls"`
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	BackOffConfig   configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	QueueSettings   configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

// TLSConfig adds custom certificates to system trust.
type TLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// Validate rejects configurations that lose completion tracking or retry bounds.
func (cfg *Config) Validate() error {
	if err := validateClusterName(cfg.ClusterName); err != nil {
		return err
	}
	if !strings.HasSuffix(cfg.Endpoint, "/stsAgent/logs/k8s") {
		return errors.New("endpoint must end in /stsAgent/logs/k8s")
	}
	if strings.TrimSpace(string(cfg.APIKey)) == "" ||
		strings.ContainsFunc(string(cfg.APIKey), func(r rune) bool { return r < 32 || r == 127 }) {
		return errors.New("api_key must contain a valid credential")
	}
	if cfg.TimeoutSettings.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	retry := cfg.BackOffConfig
	if !retry.Enabled || retry.InitialInterval <= 0 || retry.MaxInterval < retry.InitialInterval ||
		retry.MaxElapsedTime <= 0 || retry.Multiplier < 1 || math.IsNaN(retry.Multiplier) ||
		math.IsInf(retry.Multiplier, 0) || math.IsNaN(retry.RandomizationFactor) {
		return errors.New("retry_on_failure must enable finite, positive retry bounds")
	}
	if err := retry.Validate(); err != nil {
		return err
	}
	if cfg.QueueSettings.HasValue() {
		return errors.New("sending_queue must be disabled")
	}
	return nil
}

func validateClusterName(name string) error {
	if strings.TrimSpace(name) == "" || !utf8.ValidString(name) {
		return errors.New("cluster_name must be a nonempty UTF-8 string")
	}
	// The legacy Receiver strips label quotes without decoding escape sequences.
	if strings.ContainsAny(name, "\"\\") || strings.ContainsFunc(name, func(r rune) bool { return !unicode.IsPrint(r) }) {
		return errors.New("cluster_name must not require escaping in legacy log labels: " +
			"double quotes, backslashes and non-printable characters are unsupported")
	}
	return nil
}

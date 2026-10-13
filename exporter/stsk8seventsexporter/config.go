package stsk8seventsexporter

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8seventsexporter/internal/eventmapper"
)

const (
	intakeSuffix          = "/stsAgent/intake"
	clusterTypeKubernetes = "kubernetes"
	clusterTypeOpenShift  = "openshift"
)

// Config configures the cluster-agent-compatible Kubernetes events exporter.
type Config struct {
	// Endpoint is the Receiver intake URL, ending in /stsAgent/intake.
	Endpoint string              `mapstructure:"endpoint"`
	APIKey   configopaque.String `mapstructure:"api_key"`
	ProxyURL configopaque.String `mapstructure:"proxy_url"`
	TLS      TLSConfig           `mapstructure:"tls"`
	// TimeoutSettings bounds each intake request.
	TimeoutSettings exporterhelper.TimeoutConfig `mapstructure:",squash"`

	// ClusterName must match the cluster name of the Kubernetes topology, as
	// event element identifiers refer to its components.
	ClusterName string `mapstructure:"cluster_name"`
	// ClusterType is kubernetes or openshift; it sets the URN prefix and the
	// event source type.
	ClusterType string `mapstructure:"cluster_type"`
	// InternalHostname identifies the producer; the default is the pod hostname.
	InternalHostname string `mapstructure:"internal_hostname"`
	// DefaultHost is set on events without a host, which the cluster agent fills
	// with its own hostname. The default is InternalHostname.
	DefaultHost string `mapstructure:"default_host"`

	// EventCategories overrides the category per event reason.
	EventCategories map[string]string `mapstructure:"event_categories"`

	// Interval between sends of the buffered events.
	Interval time.Duration `mapstructure:"interval"`
	// MaxEventsPerRequest splits a send into several requests.
	MaxEventsPerRequest int `mapstructure:"max_events_per_request"`
	// MaxBufferedEvents bounds the events held while the intake is unavailable;
	// the oldest are dropped first.
	MaxBufferedEvents int `mapstructure:"max_buffered_events"`
	// MaxAttempts bounds the attempts per request within one send; events still
	// unsent are retried at the next interval.
	MaxAttempts int `mapstructure:"max_attempts"`
}

// TLSConfig controls server certificate verification.
type TLSConfig struct {
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
}

func (cfg *Config) Validate() error {
	if !strings.HasSuffix(cfg.Endpoint, intakeSuffix) {
		return fmt.Errorf("endpoint must end in %s", intakeSuffix)
	}
	if strings.TrimSpace(string(cfg.APIKey)) == "" ||
		strings.ContainsFunc(string(cfg.APIKey), func(r rune) bool { return r < 32 || r == 127 }) {
		return errors.New("api_key must contain a valid credential")
	}
	if strings.TrimSpace(cfg.ClusterName) == "" {
		return errors.New("cluster_name is required")
	}
	if cfg.ClusterType != clusterTypeKubernetes && cfg.ClusterType != clusterTypeOpenShift {
		return errors.New("cluster_type must be kubernetes or openshift")
	}
	for reason, category := range cfg.EventCategories {
		if !slices.Contains(eventmapper.ValidCategories, eventmapper.EventCategory(category)) {
			return fmt.Errorf("event_categories: reason %q has unknown category %q, expected one of %v",
				reason, category, eventmapper.ValidCategories)
		}
	}
	if cfg.TimeoutSettings.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if cfg.Interval <= 0 {
		return errors.New("interval must be positive")
	}
	if cfg.MaxEventsPerRequest <= 0 || cfg.MaxAttempts <= 0 {
		return errors.New("max_events_per_request and max_attempts must be positive")
	}
	if cfg.MaxBufferedEvents < cfg.MaxEventsPerRequest {
		return errors.New("max_buffered_events must not be smaller than max_events_per_request")
	}
	return nil
}

func (cfg *Config) categoryOverrides() map[string]eventmapper.EventCategory {
	out := make(map[string]eventmapper.EventCategory, len(cfg.EventCategories))
	for reason, category := range cfg.EventCategories {
		out[reason] = eventmapper.EventCategory(category)
	}
	return out
}

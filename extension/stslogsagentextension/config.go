package stslogsagentextension

import (
	"errors"
	"math"
	"net"
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	DiscoveryEnabled       bool                `mapstructure:"discovery_enabled"`
	ReceiverURL            string              `mapstructure:"receiver_url"`
	APIKey                 configopaque.String `mapstructure:"api_key"`
	ProxyURL               configopaque.String `mapstructure:"proxy_url"`
	TLS                    TLSConfig           `mapstructure:"tls"`
	StateDirectory         string              `mapstructure:"state_directory"`
	HealthEndpoint         string              `mapstructure:"health_endpoint"`
	TerminationMessagePath string              `mapstructure:"termination_message_path"`
	QueryTimeout           time.Duration       `mapstructure:"query_timeout"`
	AttemptTimeout         time.Duration       `mapstructure:"attempt_timeout"`
	MaxAttempts            int                 `mapstructure:"max_attempts"`
	InitialBackoff         time.Duration       `mapstructure:"initial_backoff"`
	MaxBackoff             time.Duration       `mapstructure:"max_backoff"`
	PollInterval           time.Duration       `mapstructure:"poll_interval"`
	Jitter                 float64             `mapstructure:"jitter"`
	StableObservations     int                 `mapstructure:"stable_observations"`
	RestartCooldown        time.Duration       `mapstructure:"restart_cooldown"`
}

type TLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.HealthEndpoint); err != nil {
		return errors.New("health_endpoint must be host:port")
	}
	if !c.DiscoveryEnabled {
		return nil
	}
	if !strings.HasSuffix(strings.TrimRight(c.ReceiverURL, "/"), "/stsAgent") {
		return errors.New("receiver_url must identify the ingest endpoint ending in /stsAgent")
	}
	if strings.TrimSpace(string(c.APIKey)) == "" {
		return errors.New("api_key is required")
	}
	if !filepath.IsAbs(c.StateDirectory) || !filepath.IsAbs(c.TerminationMessagePath) {
		return errors.New("state_directory and termination_message_path must be absolute paths")
	}
	if c.QueryTimeout <= 0 || c.AttemptTimeout <= 0 || c.AttemptTimeout > c.QueryTimeout ||
		c.MaxAttempts < 1 || c.InitialBackoff <= 0 || c.MaxBackoff < c.InitialBackoff {
		return errors.New("discovery requires positive ordered timeouts, attempts and backoff")
	}
	if c.PollInterval <= 0 || math.IsNaN(c.Jitter) || c.Jitter < 0 || c.Jitter >= 1 ||
		float64(c.PollInterval)*(1+c.Jitter) >= float64(math.MaxInt64) ||
		c.StableObservations < 2 || c.RestartCooldown <= 0 {
		return errors.New("capability polling requires finite intervals, jitter in [0,1), and at least two observations")
	}
	return nil
}

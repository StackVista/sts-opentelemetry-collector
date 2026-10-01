package stslogsagentextension

import (
	"errors"
	"net"
	"path/filepath"
	"strings"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	DiscoveryEnabled       bool                `mapstructure:"discovery_enabled"`
	FixedMode              logsagent.Mode      `mapstructure:"fixed_mode"`
	ReceiverURL            string              `mapstructure:"receiver_url"`
	APIKey                 configopaque.String `mapstructure:"api_key"`
	ProxyURL               configopaque.String `mapstructure:"proxy_url"`
	TLS                    TLSConfig           `mapstructure:"tls"`
	StateDirectory         string              `mapstructure:"state_directory"`
	HealthEndpoint         string              `mapstructure:"health_endpoint"`
	TerminationMessagePath string              `mapstructure:"termination_message_path"`
}

type TLSConfig struct {
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
}

func (c *Config) Validate() error {
	if _, _, err := net.SplitHostPort(c.HealthEndpoint); err != nil {
		return errors.New("health_endpoint must be host:port")
	}
	if !c.DiscoveryEnabled {
		if c.FixedMode != "" && c.FixedMode != logsagent.PromtailMode && c.FixedMode != logsagent.OTELNativeMode {
			return errors.New("fixed_mode must be promtail or native")
		}
		return nil
	}
	if c.FixedMode != "" {
		return errors.New("fixed_mode requires discovery_enabled to be false")
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
	return nil
}

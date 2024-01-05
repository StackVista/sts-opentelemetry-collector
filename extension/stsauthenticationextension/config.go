package stsauthenticationextension

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config specifies how the Per-RPC StackState Telemetry API token based authentication data should be obtained.
type Config struct {

	// Single token configured to validate all requests.
	Tenants map[configopaque.String]string `mapstructure:"tenants,omitempty"`
}

var _ component.Config = (*Config)(nil)
var errNoTokenProvided = errors.New("no Stackstate Telemetry API token provided")

// Validate checks if the extension configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.Tenants) == 0 {
		return errNoTokenProvided
	}

	return nil
}

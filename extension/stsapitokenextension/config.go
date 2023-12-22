package stsapitokenextension

import (
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config specifies how the Per-RPC StackState Telemetry API token based authentication data should be obtained.
type Config struct {

	// APIToken specifies the StackState Telemetry API token to use for every RPC.
	APIToken configopaque.String `mapstructure:"token,omitempty"`

	// Filename points to a file that contains the StackState Telemetry API token to use for every RPC.
	Filename string `mapstructure:"filename,omitempty"`
}

var _ component.Config = (*Config)(nil)
var errNoTokenProvided = errors.New("no Stackstate Telemetry API token provided")

// Validate checks if the extension configuration is valid
func (cfg *Config) Validate() error {
	if cfg.APIToken == "" && cfg.Filename == "" {
		return errNoTokenProvided
	}
	return nil
}

package stackstateprocessor

import (
	"fmt"

	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	// The ClusterName to tag the trace with
	ClusterName string              `mapstructure:"cluster_name"`
	ApiKey      configopaque.String `mapstructure:"api_key"`
}

func (c *Config) Validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("cluster_name is required")
	}

	if c.ApiKey == "" {
		return fmt.Errorf("api_key is required")
	}

	return nil
}

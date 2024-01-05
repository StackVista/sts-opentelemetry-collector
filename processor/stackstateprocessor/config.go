package stackstateprocessor

import (
	"fmt"
)

type Config struct {
	// The ClusterName to tag the trace with
	ClusterName string `mapstructure:"cluster_name"`
}

func (c *Config) Validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("cluster_name is required")
	}

	return nil
}

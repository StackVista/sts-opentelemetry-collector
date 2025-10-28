package ststopologyexporter

import (
	"fmt"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	// squash ensures fields correctly decoded in embedded struct.
	TimeoutSettings exporterhelper.TimeoutConfig    `mapstructure:",squash"`
	QueueSettings   exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`

	Endpoint string `mapstructure:"endpoint"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("hostname is required")
	}

	return nil
}

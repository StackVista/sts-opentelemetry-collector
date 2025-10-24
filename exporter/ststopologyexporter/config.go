package ststopologyexporter

import (
	"fmt"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	TimeoutSettings exporterhelper.TimeoutConfig    `mapstructure:",squash"` // squash ensures fields correctly decoded in embedded struct.
	QueueSettings   exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`

	Endpoint string `mapstructure:"endpoint"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("hostname is required")
	}

	return nil
}

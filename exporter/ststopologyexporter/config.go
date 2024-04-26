package ststopologyexporter

import (
	"fmt"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`

	Endpoint string `mapstructure:"endpoint"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("hostname is required")
	}

	return nil
}

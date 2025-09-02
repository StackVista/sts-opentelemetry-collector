package stskafkaexporter

import (
	"errors"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"time"
)

type Config struct {
	// squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.TimeoutSettings `mapstructure:",squash"`
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	configretry.BackOffConfig      `mapstructure:"retry_on_failure"`

	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	// ReadTimeout is the max time to wait for checking topic existence.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	// ProduceTimeout is the max time to wait for a single produce.
	ProduceTimeout time.Duration `mapstructure:"produce_timeout"`
}

func (cfg *Config) Validate() error {
	if len(cfg.Brokers) == 0 {
		return errors.New("at least one kafka broker must be specified")
	}

	if cfg.Topic == "" {
		return errors.New("'topic' must be specified")
	}

	if cfg.ReadTimeout <= 0 {
		return errors.New("'read_timeout' must be greater than 0")
	}

	if cfg.ProduceTimeout <= 0 {
		return errors.New("'produce_timeout' must be greater than 0")
	}

	return nil
}

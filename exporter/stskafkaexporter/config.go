package stskafkaexporter

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	AcksNone   = "none"
	AcksLeader = "leader"
	AcksAll    = "all"
)

type Config struct {
	// squash ensures fields are correctly decoded in embedded struct.
	TimeoutSettings           exporterhelper.TimeoutConfig    `mapstructure:",squash"`
	QueueSettings             exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	// ReadTimeout is the max time to wait for checking topic existence.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	// ProduceTimeout is the max time to wait for a single produce.
	ProduceTimeout time.Duration `mapstructure:"produce_timeout"`
	// RequiredAcks controls the acknowledgement level for produced messages.
	// Supported values: "none", "leader", "all".
	RequiredAcks string `mapstructure:"required_acks"`
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

	switch cfg.RequiredAcks {
	case AcksNone, AcksLeader, AcksAll:
		// ok
	default:
		return fmt.Errorf("invalid 'required_acks' value: '%s' (must be one of: none, leader, all)", cfg.RequiredAcks)
	}

	return nil
}

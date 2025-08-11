package internal

import (
	"errors"
	"time"
)

type Config struct {
	// File specifies the configuration for reading settings from a local file.
	File *FileSourceConfig `mapstructure:"file,omitempty"`

	// Kafka specifies the configuration for reading settings from a Kafka topic.
	Kafka *KafkaSourceConfig `mapstructure:"kafka,omitempty"`
}

type FileSourceConfig struct {
	Path string `mapstructure:"path"`

	// UpdateInterval defines how often to check for changes. Defaults to 30 seconds.
	UpdateInterval time.Duration `mapstructure:"update_interval"`
}

type KafkaSourceConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`

	// BufferSize limits the memory usage of the consumer. Defaults to 1000.
	BufferSize int `mapstructure:"buffer_size"`
}

func (cfg *Config) Validate() error {
	if cfg.File == nil && cfg.Kafka == nil {
		return errors.New("must specify either 'file' or 'kafka' configuration")
	}
	if cfg.File != nil && cfg.Kafka != nil {
		return errors.New("cannot specify both 'file' and 'kafka' configuration")
	}

	// Validate the chosen source's specific configuration.
	if cfg.File != nil {
		if cfg.File.Path == "" {
			return errors.New("'path' must be specified when using file source")
		}
		if cfg.File.UpdateInterval <= 0 {
			cfg.File.UpdateInterval = 30 * time.Second // Default
		}
	}

	if cfg.Kafka != nil {
		if len(cfg.Kafka.Brokers) == 0 {
			return errors.New("at least one kafka broker must be specified")
		}
		if cfg.Kafka.Topic == "" {
			return errors.New("'topic' must be specified when using kafka source")
		}
		if cfg.Kafka.BufferSize <= 0 {
			cfg.Kafka.BufferSize = 1000 // Default
		}
	}

	return nil
}

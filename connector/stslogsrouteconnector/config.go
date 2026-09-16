package stslogsrouteconnector

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
)

type Config struct {
	CapabilityExtension component.ID  `mapstructure:"capability_extension"`
	LegacyPipeline      pipeline.ID   `mapstructure:"legacy_pipeline"`
	NativePipeline      pipeline.ID   `mapstructure:"native_pipeline"`
	MaxConcurrentCalls  int           `mapstructure:"max_concurrent_calls"`
	MaxRecordBytes      int           `mapstructure:"max_record_bytes"`
	MaxRequestBytes     int           `mapstructure:"max_request_bytes"`
	ExportLifetime      time.Duration `mapstructure:"export_lifetime"`
}

func (c *Config) Validate() error {
	if c.CapabilityExtension.Type().String() == "" {
		return errors.New("capability_extension is required")
	}
	if c.LegacyPipeline.Signal() != pipeline.SignalLogs || c.NativePipeline.Signal() != pipeline.SignalLogs {
		return errors.New("legacy_pipeline and native_pipeline must be logs pipelines")
	}
	if c.LegacyPipeline == c.NativePipeline {
		return errors.New("legacy_pipeline and native_pipeline must differ")
	}
	if c.MaxConcurrentCalls <= 0 {
		return errors.New("max_concurrent_calls must be positive")
	}
	if c.MaxRecordBytes <= 0 || c.MaxRequestBytes < c.MaxRecordBytes {
		return errors.New("max_request_bytes must be at least max_record_bytes, and both must be positive")
	}
	if c.ExportLifetime <= 0 {
		return errors.New("export_lifetime must be positive")
	}
	return nil
}

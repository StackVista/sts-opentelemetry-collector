package stslogsrouteconnector

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
)

type Config struct {
	ControllerExtension component.ID  `mapstructure:"controller_extension"`
	PromtailPipeline    pipeline.ID   `mapstructure:"promtail_pipeline"`
	OTELNativePipeline  pipeline.ID   `mapstructure:"native_pipeline"`
	MaxConcurrentCalls  int           `mapstructure:"max_concurrent_calls"`
	MaxRecordBytes      int           `mapstructure:"max_record_bytes"`
	MaxRequestBytes     int           `mapstructure:"max_request_bytes"`
	ExportLifetime      time.Duration `mapstructure:"export_lifetime"`
}

func (c *Config) Validate() error {
	if c.ControllerExtension.Type().String() == "" {
		return errors.New("controller_extension is required")
	}
	if c.PromtailPipeline.Signal() != pipeline.SignalLogs ||
		(c.OTELNativePipeline != (pipeline.ID{}) && c.OTELNativePipeline.Signal() != pipeline.SignalLogs) {
		return errors.New("promtail_pipeline and native_pipeline must be logs pipelines")
	}
	if c.PromtailPipeline == c.OTELNativePipeline {
		return errors.New("promtail_pipeline and native_pipeline must differ")
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

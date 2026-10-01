package stslogsrouteconnector

import (
	"errors"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
	"go.opentelemetry.io/collector/pipeline"
)

type Config struct {
	logsagent.DeliveryConfig `mapstructure:",squash"`
	PromtailPipeline         pipeline.ID `mapstructure:"promtail_pipeline"`
	OTELNativePipeline       pipeline.ID `mapstructure:"native_pipeline"`
}

func (c *Config) Validate() error {
	if err := c.DeliveryConfig.Validate(); err != nil {
		return err
	}
	if c.PromtailPipeline.Signal() != pipeline.SignalLogs ||
		(c.OTELNativePipeline != (pipeline.ID{}) && c.OTELNativePipeline.Signal() != pipeline.SignalLogs) {
		return errors.New("promtail_pipeline and native_pipeline must be logs pipelines")
	}
	if c.PromtailPipeline == c.OTELNativePipeline {
		return errors.New("promtail_pipeline and native_pipeline must differ")
	}
	return nil
}

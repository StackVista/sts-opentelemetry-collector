// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stsusageprocessor // "import github.com/stackvista/sts-opentelemetry-collector/processor/stsusageprocessor"

import (
	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for Resource processor.
type Config struct {
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	return nil
}

// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package stackstateexporter

import (
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	API APIConfig `mapstructure:"api"`
}

type APIConfig struct {
	// Endpoint is the StackState endpoint to send data to.
	Endpoint string `mapstructure:"endpoint"`

	// APIKey is the StackState API key to use for authentication.
	APIKey configopaque.String `mapstructure:"api_key"`
}

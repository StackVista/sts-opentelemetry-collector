// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stsapitokenextension

import (
	"context"

	"github.com/stackvista/sts-opentelemetry-collector/extension/stsapitokenextension/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

// NewFactory creates a factory for the static bearer token Authenticator extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(_ context.Context, set extension.CreateSettings, cfg component.Config) (extension.Extension, error) {
	return newStsAPITokenAuth(cfg.(*Config), set.Logger), nil
}

package stslogsagentextension

import (
	"context"
	"errors"
	"os"
	"syscall"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(component.MustNewType("stslogsagent"), createDefaultConfig,
		createExtension, component.StabilityLevelDevelopment)
}

func createDefaultConfig() component.Config {
	return &Config{
		StateDirectory: "/var/lib/otelcol/controller", HealthEndpoint: "0.0.0.0:13133",
		TerminationMessagePath: "/dev/termination-log",
	}
}

func createExtension(_ context.Context, set extension.Settings, config component.Config) (extension.Extension, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("invalid logs capability configuration")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newController(*cfg, set, requestRestart)
}

func requestRestart() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Signal(syscall.SIGTERM)
}

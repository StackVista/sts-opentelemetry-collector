package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	//nolint:gochecknoglobals
	Type = component.MustNewType("k8scrd")
)

const (
	LogsStability = component.StabilityLevelAlpha
)

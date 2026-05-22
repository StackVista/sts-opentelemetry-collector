package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	//nolint:gochecknoglobals
	Type = component.MustNewType("k8sresource")
)

const (
	LogsStability = component.StabilityLevelAlpha
)

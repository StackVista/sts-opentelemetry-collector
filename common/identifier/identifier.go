package identifier

import (
	"context"
	"fmt"
	"io"

	"github.com/valyala/fasttemplate"
	"go.uber.org/zap"
)

const (
	StsClusterName        = "sts.cluster.name"
	StsServiceName        = "sts.service.name"
	StsPodURN             = "sts.pod.URN"
	StsServiceURN         = "sts.service.URN"
	StsServiceInstanceURN = "sts.service.instanceURN"
	K8sPodName            = "k8s.pod.name"
	K8sNamespaceName      = "k8s.namespace.name"
	PodURN                = "urn:kubernetes:/${sts.cluster.name}:${k8s.namespace.name}/pod/${k8s.pod.name}"
	ServiceURN            = "urn:kubernetes:/${sts.cluster.name}:${k8s.namespace.name}/service/${k8s.service.name}"
)

var (
	mapping = map[string]string{
		StsPodURN:     PodURN,
		StsServiceURN: ServiceURN,
	}
)

type Identifier interface {
	Identify(ctx context.Context, m map[string]string) map[string]string
}

type identifier struct {
	logger      *zap.Logger
	clusterName string
	templates   map[string]*fasttemplate.Template
}

func NewIdentifier(logger *zap.Logger, clusterName string) (*identifier, error) {
	templates, err := precompileTemplates()
	if err != nil {
		logger.Error("failed to precompile templates", zap.Error(err))
		return nil, err
	}

	return &identifier{
		logger:      logger,
		clusterName: clusterName,
		templates:   templates,
	}, nil
}

func (i *identifier) Identify(ctx context.Context, m map[string]string) map[string]string {
	out := map[string]string{}
	// Start by adding the cluster name to the map, as it is used in the templates
	m[StsClusterName] = i.clusterName

	// Add the sts cluster name attribute to the output map
	out[StsClusterName] = i.clusterName

	for k, v := range i.templates {
		id, err := v.ExecuteFuncStringWithErr(tagFunc(m))
		if err != nil {
			// OK
			continue
		}
		out[k] = id
	}

	return out
}

func tagFunc(m map[string]string) func(w io.Writer, tag string) (int, error) {
	return func(w io.Writer, tag string) (int, error) {
		v, ok := m[tag]
		if !ok {
			return 0, fmt.Errorf("tag=%q not found in attributes", tag)
		}

		if v == "" {
			return 0, nil
		}

		return w.Write([]byte(v))
	}
}

func precompileTemplates() (map[string]*fasttemplate.Template, error) {
	m := map[string]*fasttemplate.Template{}
	for k, v := range mapping {
		t, err := fasttemplate.NewTemplate(v, "${", "}")
		if err != nil {
			return nil, err
		}
		m[k] = t
	}

	return m, nil
}

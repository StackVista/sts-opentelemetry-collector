package ksm

import (
	"fmt"
	"strings"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/sender"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/servicecheck"
)

const defaultResyncPeriodSeconds = 300

// NamespaceTagger supplies the low-cardinality namespace tags that the agent
// obtained from its tagger.
type NamespaceTagger interface {
	NamespaceTags(namespace string) ([]string, error)
}

// Options carries what the agent check read from the agent configuration and
// the cluster agent.
type Options struct {
	// ClusterName is the configured cluster name; hosts are suffixed with its
	// RFC 1123 form.
	ClusterName string
	// RFC1123ClusterNameTag mirrors enabled_rfc1123_compliant_cluster_name_tag.
	RFC1123ClusterNameTag bool
	ClusterID             string
	// GlobalTags mirrors the agent's tags/extra_tags.
	GlobalTags []string
	// GlobalLabelsAsTags and GlobalAnnotationsAsTags mirror the agent's
	// kubernetes_resources_{labels,annotations}_as_tags, keyed by
	// "<plural>.<group>".
	GlobalLabelsAsTags      map[string]map[string]string
	GlobalAnnotationsAsTags map[string]map[string]string
	NamespaceTagger         NamespaceTagger
}

// New prepares the mapping configuration as the agent check's Configure does.
func New(instance *KSMConfig, opts Options) *KSMCheck {
	k := newKSMCheck(instance, opts.NamespaceTagger)
	k.clusterNameRFC1123 = strings.ReplaceAll(opts.ClusterName, "_", "-")
	k.clusterNameTagValue = opts.ClusterName
	if opts.RFC1123ClusterNameTag {
		k.clusterNameTagValue = k.clusterNameRFC1123
	}
	k.clusterIDTagValue = opts.ClusterID
	k.globalTags = opts.GlobalTags
	k.globalLabelsAsTags = opts.GlobalLabelsAsTags
	k.globalAnnotationsAsTags = opts.GlobalAnnotationsAsTags
	if k.instance.ResyncPeriod == 0 {
		k.instance.ResyncPeriod = defaultResyncPeriodSeconds
	}

	k.initTags()

	for _, joinConf := range k.instance.LabelJoins {
		joinConf.setupGetAllLabels()
	}
	k.mergeLabelJoins(defaultLabelJoins())
	k.mergeLabelsMapper(defaultLabelsMapper())
	return k
}

// resourceKindResolver replaces the agent's global API resource type cache.
type resourceKindResolver func(resource, group string) (string, error)

func newResourceKindResolver(lists []*v1.APIResourceList) resourceKindResolver {
	kinds := map[string]string{}
	for _, list := range lists {
		group := ""
		if gv := strings.SplitN(list.GroupVersion, "/", 2); len(gv) == 2 {
			group = gv[0]
		}
		for _, r := range list.APIResources {
			kinds[r.Name+"/"+group] = r.Kind
		}
	}
	return func(resource, group string) (string, error) {
		if kind, ok := kinds[resource+"/"+group]; ok {
			return kind, nil
		}
		return "", fmt.Errorf("unknown resource %q in group %q", resource, group)
	}
}

// checkTagsSender replaces the agent sender's SetCheckCustomTags.
type checkTagsSender struct {
	sender.Sender
	tags []string
}

func (s *checkTagsSender) Gauge(metric string, value float64, hostname string, tags []string) {
	s.Sender.Gauge(metric, value, hostname, append(tags, s.tags...))
}

func (s *checkTagsSender) ServiceCheck(checkName string, status servicecheck.ServiceCheckStatus, hostname string, tags []string, message string) {
	s.Sender.ServiceCheck(checkName, status, hostname, append(tags, s.tags...), message)
}

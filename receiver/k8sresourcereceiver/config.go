package k8sresourcereceiver

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type DiscoveryMode string

const (
	DiscoveryModeAPIGroups DiscoveryMode = "api_groups"
	DiscoveryModeAll       DiscoveryMode = "all"
)

// Config defines configuration for the k8sresource receiver.
// The receiver watches CRDs/CRs via informers and uses a consolidated increment loop
// to detect changes and emit snapshots.
type Config struct {
	APIConfig APIConfig `mapstructure:",squash"`

	// IncrementInterval controls how often the process cache is compared against
	// the informer cache to detect and emit changes. Default: 10s, min: 1s.
	IncrementInterval time.Duration `mapstructure:"increment_interval"`

	// SnapshotInterval controls how often a full snapshot is emitted from the informer cache.
	// Snapshots emit all current resources as ADDED (for downstream TTL freshness) and
	// emit DELETED for any resources that were previously cached but are no longer present.
	// Default: 5m, min: 1m.
	SnapshotInterval time.Duration `mapstructure:"snapshot_interval"`

	// ClusterName identifies the observed cluster. Added to log records as k8s.cluster.name.
	ClusterName string `mapstructure:"cluster_name"`

	// DiscoveryMode controls how CRDs are discovered: "api_groups" (filtered) or "all".
	DiscoveryMode DiscoveryMode `mapstructure:"discovery_mode"`

	// APIGroupFilters defines inclusion/exclusion patterns for API groups.
	// Only used when DiscoveryMode is "api_groups".
	APIGroupFilters *APIGroupFilters `mapstructure:"api_group_filters"`

	// PeerSyncPort is the port on which the HTTP server listens for peer sync requests.
	// Each replica serves its serialized cache on this port. Default: 4319.
	PeerSyncPort int `mapstructure:"peer_sync_port"`

	// PeerSyncDNS is the headless Service DNS name used to discover peer replicas.
	// Resolved via net.LookupHost to get all pod IPs behind the headless Service.
	// When empty, peer sync is disabled and the cache is not shared between replicas.
	PeerSyncDNS string `mapstructure:"peer_sync_dns"`

	// K8sLeaderElector is the component ID of a k8sleaderelector extension.
	// When set, only the leader replica actively watches CRDs/CRs.
	K8sLeaderElector *component.ID `mapstructure:"k8s_leader_elector"`

	// Objects declares Kubernetes resources to watch alongside CRD-discovered
	// custom resources. See ObjectWatch.
	Objects []ObjectWatch `mapstructure:"objects"`

	// DeniedObjects extends the built-in denylist (core Secrets and ConfigMaps)
	// with additional resources that must not appear under Objects. The built-in
	// defaults always apply and cannot be removed. Use this to block third-party
	// resources with sensitive contents (e.g. cert-manager Certificates).
	DeniedObjects []ObjectMatcher `mapstructure:"denied_objects"`
}

// ObjectMatcher identifies a Kubernetes resource by plural name and API group.
type ObjectMatcher struct {
	Name string `mapstructure:"name"`

	// Group is the API group. Empty matches the core API group.
	Group string `mapstructure:"group"`
}

// ObjectWatch declares a Kubernetes resource to watch.
//
// The shape of this type was inspired by the `K8sObjectConfig` type in the upstream `k8sobectsreceiver` -
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/receiver/k8sobjectsreceiver/config.go#L43
type ObjectWatch struct {
	// Name is the plural resource name, e.g. "pods", "deployments".
	Name string `mapstructure:"name"`

	// Group disambiguates resources that exist in multiple API groups.
	// Empty matches the core API group.
	Group string `mapstructure:"group"`

	// Version pins a specific API version. Empty uses the preferred version
	// from the Kubernetes discovery client.
	Version string `mapstructure:"version"`

	// Namespaces lists namespaces to watch. Empty means cluster-wide.
	Namespaces []string `mapstructure:"namespaces"`

	// LabelSelector is a standard k8s label selector, e.g. "app in (foo,bar)".
	LabelSelector string `mapstructure:"label_selector"`

	// FieldSelector is a standard k8s field selector, e.g. "status.phase=Running".
	FieldSelector string `mapstructure:"field_selector"`

	// gvr is resolved at startup; unexported so mapstructure ignores it.
	// nolint: unused
	gvr *schema.GroupVersionResource
}

// APIGroupFilters defines inclusion and exclusion patterns for API groups
type APIGroupFilters struct {
	// Include defines patterns to include (glob). Default: ["*"] (all groups)
	Include []string `mapstructure:"include"`

	// Exclude defines patterns to exclude (glob). Applied after include.
	Exclude []string `mapstructure:"exclude"`

	// Compiled regex caches (not exposed in config)
	includeRegexes []*regexp.Regexp
	excludeRegexes []*regexp.Regexp
}

func (c *Config) Validate() error {
	if c.IncrementInterval == 0 {
		c.IncrementInterval = 10 * time.Second
	}
	if c.IncrementInterval < 1*time.Second {
		return errors.New("increment_interval must be at least 1 second")
	}

	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 5 * time.Minute
	}
	if c.SnapshotInterval < 1*time.Minute {
		return errors.New("snapshot_interval must be at least 1 minute")
	}

	if c.SnapshotInterval < c.IncrementInterval {
		return errors.New("snapshot_interval must be greater than or equal to increment_interval")
	}

	if c.DiscoveryMode == "" {
		c.DiscoveryMode = DiscoveryModeAPIGroups
	}

	switch c.DiscoveryMode {
	case DiscoveryModeAPIGroups, DiscoveryModeAll:
		// Valid
	default:
		return fmt.Errorf("invalid discovery_mode %q: must be 'api_groups' or 'all'", c.DiscoveryMode)
	}

	if c.DiscoveryMode == DiscoveryModeAPIGroups {
		if c.APIGroupFilters == nil {
			c.APIGroupFilters = &APIGroupFilters{
				Include: []string{"*"},
				Exclude: []string{},
			}
		}

		if len(c.APIGroupFilters.Include) == 0 {
			return errors.New("api_group_filters.include cannot be empty when discovery_mode is 'api_groups'")
		}

		// Pre-compile and cache include patterns for performance
		c.APIGroupFilters.includeRegexes = make([]*regexp.Regexp, 0, len(c.APIGroupFilters.Include))
		for _, pattern := range c.APIGroupFilters.Include {
			regex, err := compilePattern(pattern)
			if err != nil {
				return fmt.Errorf("invalid include pattern %q: %w", pattern, err)
			}
			c.APIGroupFilters.includeRegexes = append(c.APIGroupFilters.includeRegexes, regex)
		}

		// Pre-compile and cache exclude patterns for performance
		c.APIGroupFilters.excludeRegexes = make([]*regexp.Regexp, 0, len(c.APIGroupFilters.Exclude))
		for _, pattern := range c.APIGroupFilters.Exclude {
			regex, err := compilePattern(pattern)
			if err != nil {
				return fmt.Errorf("invalid exclude pattern %q: %w", pattern, err)
			}
			c.APIGroupFilters.excludeRegexes = append(c.APIGroupFilters.excludeRegexes, regex)
		}
	}

	if err := c.validateObjects(); err != nil {
		return err
	}

	return nil
}

// defaultDeniedObjects are built-in denied resource types whose contents
// commonly hold sensitive material. Always applied, not removable.
//
// nolint:gochecknoglobals
var defaultDeniedObjects = []ObjectMatcher{
	{Name: "secrets", Group: ""},
	{Name: "configmaps", Group: ""},
}

// validateObjects enforces static rules on c.Objects: required name, not
// denied, parseable selectors, and uniqueness across namespace expansion.
// GVR resolution (ambiguity, missing-from-server, version-mismatch) is
// deferred to receiver startup, where the discovery client is available.
func (c *Config) validateObjects() error {
	if len(c.Objects) == 0 {
		return nil
	}
	denied := make(map[ObjectMatcher]struct{}, len(defaultDeniedObjects)+len(c.DeniedObjects))
	for _, m := range defaultDeniedObjects {
		denied[m] = struct{}{}
	}
	for _, m := range c.DeniedObjects {
		denied[m] = struct{}{}
	}
	seen := make(map[string]int, len(c.Objects))
	for i := range c.Objects {
		ow := &c.Objects[i]
		if ow.Name == "" {
			return fmt.Errorf("objects[%d]: name is required", i)
		}
		if _, blocked := denied[ObjectMatcher{Name: ow.Name, Group: ow.Group}]; blocked {
			return fmt.Errorf("objects[%d]: resource %q in group %q is denied (sensitive content)",
				i, ow.Name, ow.Group)
		}
		if ow.LabelSelector != "" {
			if _, err := labels.Parse(ow.LabelSelector); err != nil {
				return fmt.Errorf("objects[%d]: invalid label_selector %q: %w", i, ow.LabelSelector, err)
			}
		}
		if ow.FieldSelector != "" {
			if _, err := fields.ParseSelector(ow.FieldSelector); err != nil {
				return fmt.Errorf("objects[%d]: invalid field_selector %q: %w", i, ow.FieldSelector, err)
			}
		}
		namespaces := ow.Namespaces
		if len(namespaces) == 0 {
			namespaces = []string{""}
		}
		for _, ns := range namespaces {
			key := strings.Join([]string{ow.Name, ow.Group, ow.Version, ns, ow.LabelSelector, ow.FieldSelector}, "|")
			if prev, ok := seen[key]; ok {
				return fmt.Errorf("objects[%d] duplicates objects[%d]: name=%q group=%q version=%q namespace=%q",
					i, prev, ow.Name, ow.Group, ow.Version, ns)
			}
			seen[key] = i
		}
	}
	return nil
}

func (c *Config) getDynamicClient() (dynamic.Interface, error) {
	return MakeDynamicClient(c.APIConfig)
}

// shouldWatchAPIGroup determines if a CRD's API group should be watched based on filters
func (c *Config) shouldWatchAPIGroup(apiGroup string) bool {
	if c.DiscoveryMode == DiscoveryModeAll {
		return true
	}

	// Check include patterns using cached compiled regexes
	included := false
	for _, regex := range c.APIGroupFilters.includeRegexes {
		if regex.MatchString(apiGroup) {
			included = true
			break
		}
	}

	if !included {
		return false
	}

	// Check exclude patterns using cached compiled regexes
	for _, regex := range c.APIGroupFilters.excludeRegexes {
		if regex.MatchString(apiGroup) {
			return false
		}
	}

	return true
}

// compilePattern converts a glob-style pattern to a regex
func compilePattern(pattern string) (*regexp.Regexp, error) {
	// Escape regex special chars except *
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped \* with .*
	regexPattern := "^" + strings.ReplaceAll(escaped, `\*`, `.*`) + "$"

	return regexp.Compile(regexPattern)
}

//nolint:testpackage // Tests require access to internal functions
package k8sresourcereceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with api_groups mode",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{"*"},
					Exclude: []string{},
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with all mode",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			wantErr: false,
		},
		{
			name: "valid config with specific API groups",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{testPoliciesKubewarden, testSuseWildcard},
					Exclude: []string{"internal.suse.com"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty include patterns",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{},
					Exclude: []string{},
				},
			},
			wantErr: true,
			errMsg:  "cr_api_groups.include cannot be empty",
		},
		{
			name: "invalid discovery mode",
			config: &Config{
				DiscoveryMode: testInvalid,
			},
			wantErr: true,
			errMsg:  "invalid discovery_mode",
		},
		// Note: glob patterns with special chars are escaped by QuoteMeta,
		// so they become literal matches and won't fail validation.
		// Regex compilation only fails on invalid constructed patterns.
		{
			name: "empty discovery mode defaults to api_groups",
			config: &Config{
				DiscoveryMode: "",
			},
			wantErr: false,
		},
		{
			name: "nil cr_api_groups gets default",
			config: &Config{
				DiscoveryMode:           DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: nil,
			},
			wantErr: false,
		},
		{
			name: "zero increment_interval gets default",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			wantErr: false,
		},
		{
			name: "increment_interval below 1s rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 500 * time.Millisecond,
				SnapshotInterval:  5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "increment_interval must be at least 1 second",
		},
		{
			name: "snapshot_interval below 1m rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 10 * time.Second,
				SnapshotInterval:  30 * time.Second,
			},
			wantErr: true,
			errMsg:  "snapshot_interval must be at least 1 minute",
		},
		{
			name: "snapshot_interval below increment_interval rejected",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 10 * time.Minute,
				SnapshotInterval:  5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "snapshot_interval must be greater than or equal to increment_interval",
		},
		{
			name: "snapshot_interval equal to increment_interval accepted",
			config: &Config{
				DiscoveryMode:     DiscoveryModeAll,
				IncrementInterval: 1 * time.Minute,
				SnapshotInterval:  1 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "valid objects watch on core pods",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", Namespaces: []string{"runtime-enforcer"}, LabelSelector: "app=foo"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid objects watch on grouped deployments",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "deployments", Group: "apps", Namespaces: []string{"runtime-enforcer"}},
				},
			},
			wantErr: false,
		},
		{
			name: "valid cluster-wide objects watch with empty namespaces",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "namespaces"},
				},
			},
			wantErr: false,
		},
		{
			name: "objects entry missing name rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Namespaces: []string{"runtime-enforcer"}},
				},
			},
			wantErr: true,
			errMsg:  "objects[0]: name is required",
		},
		{
			name: "objects entry with invalid label_selector rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", LabelSelector: "!!!invalid==="},
				},
			},
			wantErr: true,
			errMsg:  "invalid label_selector",
		},
		{
			name: "objects entry with invalid field_selector rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", FieldSelector: "status.phase!!Running"},
				},
			},
			wantErr: true,
			errMsg:  "invalid field_selector",
		},
		{
			name: "objects exact-duplicate rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", Namespaces: []string{"a"}},
					{Name: "pods", Namespaces: []string{"a"}},
				},
			},
			wantErr: true,
			errMsg:  "objects[1] duplicates objects[0]",
		},
		{
			name: "objects namespace-overlap rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", Namespaces: []string{"a", "b"}},
					{Name: "pods", Namespaces: []string{"b"}},
				},
			},
			wantErr: true,
			errMsg:  "objects[1] duplicates objects[0]",
		},
		{
			name: "objects with different namespaces accepted",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", Namespaces: []string{"a"}},
					{Name: "pods", Namespaces: []string{"b"}},
				},
			},
			wantErr: false,
		},
		{
			name: "objects with different selectors accepted",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "pods", Namespaces: []string{"a"}, LabelSelector: "app=foo"},
					{Name: "pods", Namespaces: []string{"a"}, LabelSelector: "app=bar"},
				},
			},
			wantErr: false,
		},
		{
			name: "objects cluster-wide duplicate rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "namespaces"},
					{Name: "namespaces"},
				},
			},
			wantErr: true,
			errMsg:  "objects[1] duplicates objects[0]",
		},
		{
			name: "core secrets denied by default",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: resourceSecrets},
				},
			},
			wantErr: true,
			errMsg:  `resource "secrets" in group "" is denied`,
		},
		{
			name: "core configmaps denied by default",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: "configmaps", Namespaces: []string{"runtime-enforcer"}},
				},
			},
			wantErr: true,
			errMsg:  `resource "configmaps" in group "" is denied`,
		},
		{
			name: "third-party resource named secrets allowed when group differs",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				Objects: []ObjectWatch{
					{Name: resourceSecrets, Group: "vault.example.com"},
				},
			},
			wantErr: false,
		},
		{
			name: "denied_objects extension blocks cert-manager certificates",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				DeniedObjects: []ObjectMatcher{
					{Name: "certificates", Group: "cert-manager.io"},
				},
				Objects: []ObjectWatch{
					{Name: "certificates", Group: "cert-manager.io"},
				},
			},
			wantErr: true,
			errMsg:  `resource "certificates" in group "cert-manager.io" is denied`,
		},
		{
			name: "denied_objects cannot remove built-in defaults",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				DeniedObjects: []ObjectMatcher{
					{Name: "events", Group: ""},
				},
				Objects: []ObjectWatch{
					{Name: "secrets"},
				},
			},
			wantErr: true,
			errMsg:  `resource "secrets" in group "" is denied`,
		},
		{
			name: "denied_objects entry without name rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				DeniedObjects: []ObjectMatcher{
					{Group: "cert-manager.io"},
				},
			},
			wantErr: true,
			errMsg:  "denied_objects[0]: name is required",
		},
		{
			name: "peer_sync_port negative rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				PeerSyncPort:  -1,
			},
			wantErr: true,
			errMsg:  "peer_sync_port must be between 0 and 65535",
		},
		{
			name: "peer_sync_port above 65535 rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				PeerSyncPort:  65536,
			},
			wantErr: true,
			errMsg:  "peer_sync_port must be between 0 and 65535",
		},
		{
			name: "peer_sync_port zero accepted",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				PeerSyncPort:  0,
			},
			wantErr: false,
		},
		{
			name: "valid resource_attributes with api_groups filter",
			config: &Config{
				DiscoveryMode:      DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{validResourceAttr()},
			},
			wantErr: false,
		},
		{
			name: "valid resource_attributes with resources filter only",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withApplyTo(validResourceAttr(), ResourceAttributeApplyTo{Resources: []string{"virtualmachines"}}),
				},
			},
			wantErr: false,
		},
		{
			name: "valid resource_attributes with both api_groups and resources",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withApplyTo(validResourceAttr(), ResourceAttributeApplyTo{
						APIGroups: []string{"kubevirt.io"},
						Resources: []string{"virtualmachines"},
					}),
				},
			},
			wantErr: false,
		},
		{
			name: "resource_attributes key required",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withKey(validResourceAttr(), ""),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].key is required",
		},
		{
			name: "resource_attributes reserved key k8s.cluster.name rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withKey(validResourceAttr(), "k8s.cluster.name"),
				},
			},
			wantErr: true,
			errMsg:  `resource_attributes[0].key "k8s.cluster.name" is reserved`,
		},
		{
			name: "resource_attributes reserved key k8s.namespace.name rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withKey(validResourceAttr(), "k8s.namespace.name"),
				},
			},
			wantErr: true,
			errMsg:  `resource_attributes[0].key "k8s.namespace.name" is reserved`,
		},
		{
			name: "resource_attributes duplicate key rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					validResourceAttr(),
					validResourceAttr(),
				},
			},
			wantErr: true,
			errMsg:  `resource_attributes[1].key duplicates resource_attributes[0].key`,
		},
		{
			name: "resource_attributes object.name required",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withObjectName(validResourceAttr(), ""),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].value_from.object.name is required",
		},
		{
			name: "resource_attributes object.resource required",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withObjectResource(validResourceAttr(), ""),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].value_from.object.resource is required",
		},
		{
			name: "resource_attributes object.namespace required",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withObjectNamespace(validResourceAttr(), ""),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].value_from.object.namespace is required",
		},
		{
			name: "resource_attributes apply_to must have api_groups or resources",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withApplyTo(validResourceAttr(), ResourceAttributeApplyTo{}),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].apply_to.api_groups or resource_attributes[0].apply_to.resources is required",
		},
		{
			name: "resource_attributes blank api_groups entry rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withApplyTo(validResourceAttr(), ResourceAttributeApplyTo{APIGroups: []string{"kubevirt.io", ""}}),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].apply_to.api_groups[1] must not be empty",
		},
		{
			name: "resource_attributes blank resources entry rejected",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
				ResourceAttributes: []ResourceAttributeEnrichment{
					withApplyTo(validResourceAttr(), ResourceAttributeApplyTo{Resources: []string{"virtualmachines", ""}}),
				},
			},
			wantErr: true,
			errMsg:  "resource_attributes[0].apply_to.resources[1] must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRancherEnrichmentInjectedWhenEnabled(t *testing.T) {
	cfg := &Config{
		DiscoveryMode:     DiscoveryModeAll,
		RancherEnrichment: rancherEnrichmentConfig{Enabled: true},
	}
	require.NoError(t, cfg.Validate())

	require.Len(t, cfg.ResourceAttributes, 2)
	ra := cfg.ResourceAttributes[0]
	assert.Equal(t, rancherManagerURLKey, ra.Key())
	src := ra.ValueFrom()
	assert.Equal(t, agentDeploymentName, src.Object.Name)
	assert.Equal(t, rancherAPIGroup, src.Object.Group)
	assert.Equal(t, agentResource, src.Object.Resource)
	assert.Equal(t, rancherNamespace, src.Object.Namespace)
	assert.Equal(t, []string{"*"}, ra.ApplyTo().APIGroups)
}

func TestRancherEnrichmentNotInjectedWhenDisabled(t *testing.T) {
	cfg := &Config{
		DiscoveryMode:     DiscoveryModeAll,
		RancherEnrichment: rancherEnrichmentConfig{Enabled: false},
	}
	require.NoError(t, cfg.Validate())
	assert.Empty(t, cfg.ResourceAttributes)
}

func TestRancherEnrichmentCoexistsWithUserResourceAttributes(t *testing.T) {
	cfg := &Config{
		DiscoveryMode:     DiscoveryModeAll,
		RancherEnrichment: rancherEnrichmentConfig{Enabled: true},
		ResourceAttributes: []ResourceAttributeEnrichment{
			validResourceAttr(),
		},
	}
	require.NoError(t, cfg.Validate())
	// User entry preserved; rancher entry appended.
	require.Len(t, cfg.ResourceAttributes, 3)
	assert.Equal(t, "my.attr", cfg.ResourceAttributes[0].Key())
	assert.Equal(t, rancherManagerURLKey, cfg.ResourceAttributes[1].Key())
	assert.Equal(t, rancherClusterIDKey, cfg.ResourceAttributes[2].Key())
}

type TestResourceAttributeEnrichment struct {
	TestKey string `mapstructure:"key"`

	TestValueFrom ResourceAttributeValueFrom `mapstructure:"value_from"`

	// Extract(unstructured *unstructured.Unstructured) (string, bool, bool)

	TestApplyTo ResourceAttributeApplyTo `mapstructure:"apply_to"`

	K8sContainerEnv *K8sContainerEnvSource `mapstructure:"k8s_container_env"`
}

// K8sContainerEnvSource extracts a static env[].value from a container in a Kubernetes object.
type K8sContainerEnvSource struct {
	Container string `mapstructure:"container"`
	Env       string `mapstructure:"env"`
}

func (r TestResourceAttributeEnrichment) Key() string {
	return r.TestKey
}

func (r TestResourceAttributeEnrichment) ValueFrom() ResourceAttributeValueFrom {
	return r.TestValueFrom
}

func (r TestResourceAttributeEnrichment) ApplyTo() ResourceAttributeApplyTo {
	return r.TestApplyTo
}

func (r TestResourceAttributeEnrichment) Extract(_ *unstructured.Unstructured) (string, bool, bool) {
	return "", false, false
}

// validResourceAttr returns a fully-populated ResourceAttributeEnrichment that passes validation.
// Individual test cases mutate specific fields via the with* helpers.
func validResourceAttr() TestResourceAttributeEnrichment {
	return TestResourceAttributeEnrichment{
		TestKey: "my.attr",
		TestValueFrom: ResourceAttributeValueFrom{
			Object: K8sObjectSource{
				Name:      "my-deployment",
				Resource:  "deployments",
				Namespace: "my-namespace",
			},
		},
		K8sContainerEnv: &K8sContainerEnvSource{
			Container: "my-container",
			Env:       "MY_ENV_VAR",
		},
		TestApplyTo: ResourceAttributeApplyTo{
			APIGroups: []string{"kubevirt.io"},
		},
	}
}

func withKey(r TestResourceAttributeEnrichment, key string) TestResourceAttributeEnrichment {
	r.TestKey = key
	return r
}

func withApplyTo(r TestResourceAttributeEnrichment, applyTo ResourceAttributeApplyTo) TestResourceAttributeEnrichment {
	r.TestApplyTo = applyTo
	return r
}

func withObjectName(r TestResourceAttributeEnrichment, name string) TestResourceAttributeEnrichment {
	r.TestValueFrom.Object.Name = name
	return r
}

func withObjectResource(r TestResourceAttributeEnrichment, resource string) TestResourceAttributeEnrichment {
	r.TestValueFrom.Object.Resource = resource
	return r
}

func withObjectNamespace(r TestResourceAttributeEnrichment, namespace string) TestResourceAttributeEnrichment {
	r.TestValueFrom.Object.Namespace = namespace
	return r
}

// TestCompiledPatternMatches verifies that the glob-to-regex translation done by compilePattern
// produces the expected match/non-match behaviour. The test exercises compilePattern directly
// (instead of going through Config.shouldWatchAPIGroup) so each glob feature can be pinned down
// in isolation.
func TestCompiledPatternMatches(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		want    bool
	}{
		// Wildcard tests
		{"*", "anything", true},
		{"*", testExampleGroup, true},
		{"*", "", true},

		// Subdomain wildcard tests
		{testExampleWildcard, "foo.example.com", true},
		{testExampleWildcard, "bar.example.com", true},
		{testExampleWildcard, "nested.foo.example.com", true},
		{testExampleWildcard, testExampleGroup, false}, // Should NOT match bare domain

		// Exact match tests
		{testExactMatchIO, testExactMatchIO, true},
		{testExactMatchIO, "not.exact.match.io", false},
		{testExactMatchIO, "exact.match", false},

		// Multiple wildcard tests
		{testFooWildcardBar, "foo.baz.bar", true},
		{testFooWildcardBar, "foo.anything.bar", true},
		{testFooWildcardBar, "foo.nested.path.bar", true},
		{testFooWildcardBar, "foo.bar", false}, // * requires at least one char

		// Complex patterns
		{testPoliciesWildcardIO, testPoliciesKubewarden, true},
		{testPoliciesWildcardIO, "policies.example.io", true},
		{testPoliciesWildcardIO, "policies.io", false},

		// Special characters in domain (should be escaped, not interpreted as regex)
		{testExampleGroup, "exampleXcom", false}, // . should not match any char
		{"test-domain.io", "test-domain.io", true},
		{"test_domain.io", "test_domain.io", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+" matches "+tt.str, func(t *testing.T) {
			regex, err := compilePattern(tt.pattern)
			require.NoError(t, err)
			got := regex.MatchString(tt.str)
			assert.Equal(t, tt.want, got, "pattern=%q str=%q", tt.pattern, tt.str)
		})
	}
}

func TestCompilePattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{"wildcard", "*", false},
		{"subdomain wildcard", "*.example.com", false},
		{"exact match", "exact.match", false},
		{"multiple wildcards", "foo.*.bar.*", false},
		{"special chars", "test-domain_name.io", false},
		{"empty pattern", "", false},
		// Note: All glob patterns should compile successfully
		// Invalid regex would only come from malformed brackets, which QuoteMeta handles
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			regex, err := compilePattern(tt.pattern)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, regex)
			}
		})
	}
}

func TestShouldWatchAPIGroup(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		apiGroup string
		want     bool
	}{
		{
			name: "all mode watches everything",
			config: &Config{
				DiscoveryMode: DiscoveryModeAll,
			},
			apiGroup: "anything.io",
			want:     true,
		},
		{
			name: "wildcard includes everything",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{"*"},
				},
			},
			apiGroup: testExampleGroup,
			want:     true,
		},
		{
			name: "exact match included",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{testPoliciesKubewarden},
				},
			},
			apiGroup: testPoliciesKubewarden,
			want:     true,
		},
		{
			name: "exact match not included",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{testPoliciesKubewarden},
				},
			},
			apiGroup: "other.io",
			want:     false,
		},
		{
			name: "subdomain wildcard match",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{testSuseWildcard},
				},
			},
			apiGroup: "rancher.suse.com",
			want:     true,
		},
		{
			name: "excluded group not watched",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{"*"},
					Exclude: []string{"internal.example.com"},
				},
			},
			apiGroup: "internal.example.com",
			want:     false,
		},
		{
			name: "excluded wildcard pattern",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{testExampleWildcard},
					Exclude: []string{"test.*.example.com"},
				},
			},
			apiGroup: "test.foo.example.com",
			want:     false,
		},
		{
			name: "multiple include patterns",
			config: &Config{
				DiscoveryMode: DiscoveryModeAPIGroups,
				CustomResourceAPIGroups: &APIGroups{
					Include: []string{
						testPoliciesKubewarden,
						"longhorn.io",
						testSuseWildcard,
					},
				},
			},
			apiGroup: "rancher.suse.com",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call Validate to compile and cache regex patterns
			err := tt.config.Validate()
			require.NoError(t, err)

			got := tt.config.shouldWatchAPIGroup(tt.apiGroup)
			assert.Equal(t, tt.want, got)
		})
	}
}

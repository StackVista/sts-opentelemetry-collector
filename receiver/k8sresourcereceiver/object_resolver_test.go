//nolint:testpackage // Tests require access to internal functions
package k8sresourcereceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// stubDiscovery is a minimal discovery.DiscoveryInterface for tests. Only the
// methods called by resolveObjectGVRs are implemented; others will nil-panic
// if invoked, which is intentional (catches accidental dependencies).
type stubDiscovery struct {
	discovery.DiscoveryInterface
	resources []*metav1.APIResourceList
	err       error
}

func (s *stubDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return s.resources, s.err
}

func TestResolveObjectGVRs(t *testing.T) {
	coreV1 := &metav1.APIResourceList{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Namespaced: true},
			{Name: "namespaces", Namespaced: false},
			{Name: "events", Namespaced: true},
		},
	}
	appsV1 := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Namespaced: true},
		},
	}
	eventsV1 := &metav1.APIResourceList{
		GroupVersion: "events.k8s.io/v1",
		APIResources: []metav1.APIResource{
			{Name: "events", Namespaced: true},
		},
	}
	allResources := []*metav1.APIResourceList{coreV1, appsV1, eventsV1}

	tests := []struct {
		name          string
		watches       []ObjectWatch
		discResources []*metav1.APIResourceList
		discErr       error
		wantErr       bool
		errMsg        string
		wantGVRs      []schema.GroupVersionResource
	}{
		{
			name:          "core pods resolve to /v1/pods",
			watches:       []ObjectWatch{{Name: "pods", Namespaces: []string{"ns-a"}}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "pods"}},
		},
		{
			name:          "grouped deployments resolve to apps/v1/deployments",
			watches:       []ObjectWatch{{Name: "deployments", Group: "apps"}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "apps", Version: "v1", Resource: "deployments"}},
		},
		{
			name:          "core preferred when plural exists in core and a non-core group",
			watches:       []ObjectWatch{{Name: "events"}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "events"}},
		},
		{
			name:          "explicit empty group selects core (same path as omitted)",
			watches:       []ObjectWatch{{Name: "events", Group: ""}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "events"}},
		},
		{
			name:          "explicit non-core group disambiguates events",
			watches:       []ObjectWatch{{Name: "events", Group: "events.k8s.io"}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "events.k8s.io", Version: "v1", Resource: "events"}},
		},
		{
			name:          "single non-core candidate resolves without explicit group",
			watches:       []ObjectWatch{{Name: "deployments"}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "apps", Version: "v1", Resource: "deployments"}},
		},
		{
			name: "multiple non-core candidates without group fail with ambiguity error",
			watches: []ObjectWatch{{Name: "widgets"}},
			discResources: []*metav1.APIResourceList{
				{
					GroupVersion: "foo.example.com/v1",
					APIResources: []metav1.APIResource{{Name: "widgets", Namespaced: true}},
				},
				{
					GroupVersion: "bar.example.com/v1",
					APIResources: []metav1.APIResource{{Name: "widgets", Namespaced: true}},
				},
			},
			wantErr: true,
			errMsg:  `resource "widgets" is ambiguous across non-core groups (bar.example.com, foo.example.com)`,
		},
		{
			name:          "missing resource error mentions RBAC",
			watches:       []ObjectWatch{{Name: "nonexistent"}},
			discResources: allResources,
			wantErr:       true,
			errMsg:        "missing RBAC list permission",
		},
		{
			name:          "explicit version not exposed by server is rejected",
			watches:       []ObjectWatch{{Name: "pods", Version: "v999"}},
			discResources: allResources,
			wantErr:       true,
			errMsg:        `version "v999" not exposed by the server`,
		},
		{
			name:          "cluster-scoped resource with namespaces is rejected",
			watches:       []ObjectWatch{{Name: "namespaces", Namespaces: []string{"foo"}}},
			discResources: allResources,
			wantErr:       true,
			errMsg:        "cluster-scoped; remove namespaces",
		},
		{
			name:          "cluster-scoped resource without namespaces resolves",
			watches:       []ObjectWatch{{Name: "namespaces"}},
			discResources: allResources,
			wantGVRs:      []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "namespaces"}},
		},
		{
			name:          "partial discovery failure tolerated when requested resource resolves",
			watches:       []ObjectWatch{{Name: "pods"}},
			discResources: []*metav1.APIResourceList{coreV1},
			discErr: &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
				{Group: "metrics.k8s.io", Version: "v1beta1"}: assert.AnError,
			}},
			wantGVRs: []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "pods"}},
		},
		{
			name:    "empty watches is a no-op",
			watches: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disc := &stubDiscovery{resources: tt.discResources, err: tt.discErr}
			resolved, err := resolveObjectGVRs(disc, tt.watches, zap.NewNop())
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Len(t, resolved, len(tt.wantGVRs))
			for i, want := range tt.wantGVRs {
				assert.Equal(t, want, resolved[i].GVR, "watch %d", i)
			}
		})
	}
}

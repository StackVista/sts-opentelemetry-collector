//nolint:testpackage
package k8sresourcereceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestResourceAttributeApplyToMatchesGroupsAndResources(t *testing.T) {
	applyTo := ResourceAttributeApplyTo{APIGroups: []string{"kubevirt.io"}, Resources: []string{"virtualmachines"}}

	assert.True(t, applyTo.matches(schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}))
	assert.False(t, applyTo.matches(schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "pods"}))
	assert.False(t, applyTo.matches(schema.GroupVersionResource{Group: "harvesterhci.io", Version: "v1", Resource: "virtualmachines"}))
}

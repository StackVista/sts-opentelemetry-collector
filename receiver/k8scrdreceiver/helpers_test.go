//nolint:testpackage
package k8scrdreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestGetStorageVersion(t *testing.T) {
	tests := []struct {
		name string
		crd  *apiextensionsv1.CustomResourceDefinition
		want string
	}{
		{
			name: "single storage version",
			crd: &apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1", Storage: true},
					},
				},
			},
			want: "v1",
		},
		{
			name: "multiple versions, one storage",
			crd: &apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1alpha1", Storage: false},
						{Name: "v1beta1", Storage: false},
						{Name: "v1", Storage: true},
					},
				},
			},
			want: "v1",
		},
		{
			name: "no storage version marked, returns first",
			crd: &apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1alpha1", Storage: false},
						{Name: "v1", Storage: false},
					},
				},
			},
			want: "v1alpha1",
		},
		{
			name: "no versions",
			crd: &apiextensionsv1.CustomResourceDefinition{
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{},
				},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getStorageVersion(tt.crd)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConvertUnstructuredToCRD(t *testing.T) {
	tests := []struct {
		name    string
		input   *unstructured.Unstructured
		wantErr bool
	}{
		{
			name: "valid CRD object",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]interface{}{
						"name": "policiesservers.policies.kubewarden.io",
					},
					"spec": map[string]interface{}{
						"group": "policies.kubewarden.io",
						"names": map[string]interface{}{
							"kind":   "PolicyServer",
							"plural": "policyservers",
						},
						"scope": "Namespaced",
						"versions": []interface{}{
							map[string]interface{}{
								"name":    "v1",
								"storage": true,
								"served":  true,
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid JSON structure",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"invalid": make(chan int), // Cannot be marshaled to JSON
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var crd apiextensionsv1.CustomResourceDefinition
			err := convertUnstructuredToCRD(tt.input, &crd)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.name == "valid CRD object" {
					assert.Equal(t, "policies.kubewarden.io", crd.Spec.Group)
					assert.Equal(t, "PolicyServer", crd.Spec.Names.Kind)
				}
			}
		})
	}
}

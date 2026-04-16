package k8scrdreceiver

import (
	"encoding/json"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// getStorageVersion returns the storage version of a CRD.
// If no storage version is marked, it returns the first version.
// Returns empty string if the CRD has no versions.
func getStorageVersion(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, version := range crd.Spec.Versions {
		if version.Storage {
			return version.Name
		}
	}

	// Fallback to first version if no storage version marked
	if len(crd.Spec.Versions) > 0 {
		return crd.Spec.Versions[0].Name
	}

	return ""
}

// convertUnstructuredToCRD converts an Unstructured object to a CRD.
func convertUnstructuredToCRD(u *unstructured.Unstructured, crd *apiextensionsv1.CustomResourceDefinition) error {
	bytes, err := u.MarshalJSON()
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, crd)
}

// isPermissionDenied checks if an error is a Kubernetes RBAC permission denied error.
func isPermissionDenied(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err)
}

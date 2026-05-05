//nolint:testpackage // Tests require access to internal receiver functions
package k8scrdreceiver

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIsPermissionDenied(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "forbidden error",
			err: apierrors.NewForbidden(
				schema.GroupResource{Group: "policies.kubewarden.io", Resource: "policyservers"},
				"test-resource",
				errors.New("user cannot watch resource"),
			),
			want: true,
		},
		{
			name: "unauthorized error",
			err: apierrors.NewUnauthorized(
				"user is not authorized",
			),
			want: true,
		},
		{
			name: "other status error",
			err: apierrors.NewInternalError(
				errors.New("internal server error"),
			),
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPermissionDenied(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

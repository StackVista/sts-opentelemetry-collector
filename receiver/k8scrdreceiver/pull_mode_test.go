//nolint:testpackage // Tests require access to internal functions
package k8scrdreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func newTestPullMode(client k8sClient, sink *consumertest.LogsSink, config *Config) *pullMode {
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}
	return newPullMode(settings, config, sink, client, newForbiddenTracker(1*time.Hour))
}

func TestPullMode_PerformPull(t *testing.T) {
	crd1 := makeTestCRD("test-crd-1", "example.com", "TestResource", "testresources")
	crd2 := makeTestCRD("test-crd-2", "excluded.com", "OtherResource", "otherresources")
	cr1 := makeTestCR("test-cr-1", "example.com", "TestResource", "v1")
	cr2 := makeTestCR("test-cr-2", "example.com", "TestResource", "v1")

	client := newFakeClient().
		withList(crdGVR, &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{*crd1, *crd2},
		}).
		withList(schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}, &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{*cr1, *cr2},
		})

	config := &Config{
		Pull:          PullConfig{Enabled: true, Interval: 1 * time.Minute},
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
			Exclude: []string{},
		},
	}
	require.NoError(t, config.Validate())

	sink := &consumertest.LogsSink{}
	pm := newTestPullMode(client, sink, config)
	pm.ctx = context.Background()

	err := pm.performPull()
	require.NoError(t, err)

	// 2 CRDs (all emitted) + 2 CRs (from included group only) = 4
	assert.Equal(t, 4, sink.LogRecordCount())
}

func TestPullMode_EmitLog(t *testing.T) {
	cr := makeTestCR("test-cr", "example.com", "TestResource", "v1")

	sink := &consumertest.LogsSink{}
	err := emitLog(context.Background(), sink, cr, "ADDED", buildCRLogRecord)
	require.NoError(t, err)
	assert.Equal(t, 1, sink.LogRecordCount())
}

func TestPullMode_PullCRsForCRD_RespectsFilters(t *testing.T) {
	crd := makeTestCRD("test-crd", "example.com", "TestResource", "testresources")
	cr1 := makeTestCR("test-cr-1", "example.com", "TestResource", "v1")
	cr2 := makeTestCR("test-cr-2", "example.com", "TestResource", "v1")

	client := newFakeClient().
		withList(schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}, &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{*cr1, *cr2},
		})

	config := &Config{
		DiscoveryMode: DiscoveryModeAPIGroups,
		APIGroupFilters: &APIGroupFilters{
			Include: []string{"example.com"},
		},
	}

	sink := &consumertest.LogsSink{}
	pm := newTestPullMode(client, sink, config)
	pm.ctx = context.Background()

	err := pm.pullCRsForCRD(crd)
	require.NoError(t, err)
	assert.Equal(t, 2, sink.LogRecordCount())
}

func TestPullMode_PullCRsForCRD_HandlesPermissionDenied(t *testing.T) {
	crd := makeTestCRD("test-crd", "forbidden.com", "ForbiddenResource", "forbiddenresources")

	client := newFakeClient().
		withListError(
			schema.GroupVersionResource{Group: "forbidden.com", Version: "v1", Resource: "forbiddenresources"},
			apierrors.NewForbidden(
				schema.GroupResource{Group: "forbidden.com", Resource: "forbiddenresources"},
				"",
				errors.New("permission denied"),
			),
		)

	sink := &consumertest.LogsSink{}
	pm := newTestPullMode(client, sink, &Config{})
	pm.ctx = context.Background()

	err := pm.pullCRsForCRD(crd)
	require.NoError(t, err)
	assert.Equal(t, 0, sink.LogRecordCount())

	gvr := schema.GroupVersionResource{Group: "forbidden.com", Version: "v1", Resource: "forbiddenresources"}
	shouldRetry, _ := pm.forbiddenTracker.shouldRetry(gvr)
	assert.False(t, shouldRetry, "resource should be marked as forbidden")
}


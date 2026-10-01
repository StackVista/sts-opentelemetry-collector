package stsk8stopologyexporter //nolint:testpackage // Feeds the internal store and collectors with observer records.

import (
	"context"
	"testing"
	"time"

	collectors "github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/topologycollectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestInlineVolumeAttachmentIsIgnored(t *testing.T) {
	inline := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.k8s.io/v1",
		"kind":       "VolumeAttachment",
		"metadata":   map[string]any{"name": "csi-inline", "uid": "uid-csi-inline"},
		"spec": map[string]any{
			"attacher": "ebs.csi.aws.com",
			"nodeName": "node-a",
			"source": map[string]any{"inlineVolumeSpec": map[string]any{
				"csi": map[string]any{"driver": "ebs.csi.aws.com", "volumeHandle": "vol-inline"},
			}},
		},
	}}
	cfg := testConfig()
	exp := newTopologyExporter(cfg, zaptest.NewLogger(t), nil)
	for _, logs := range snapshotRecords(t, "1", append(loadFixture(t), inline)) {
		require.NoError(t, exp.consumeLogs(context.Background(), logs))
	}
	objects, _, ok := exp.store.view(time.Now(), cfg.SnapshotMaxAge)
	require.True(t, ok)
	result, err := collectTopology(&cacheClient{objects: objects, logger: exp.logger},
		exp.instance, collectors.Kubernetes, cfg, exp.logger)
	require.NoError(t, err)

	golden := loadGolden(t)
	assert.Equal(t, golden.Components, normalize(t, result.components))
	assert.Equal(t, golden.Relations, normalize(t, result.relations))
}

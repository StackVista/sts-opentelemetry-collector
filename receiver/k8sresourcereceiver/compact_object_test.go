//nolint:testpackage
package k8sresourcereceiver

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// forEachCacheMode runs f with CompactCache off and on.
func forEachCacheMode(t *testing.T, f func(t *testing.T, compact bool)) {
	t.Helper()
	for _, compact := range []bool{false, true} {
		name := "live"
		if compact {
			name = "compact"
		}
		t.Run(name, func(t *testing.T) { f(t, compact) })
	}
}

// forEachPeerCacheMode runs f for every sender/receiver CompactCache pairing,
// which mixed replicas meet while the setting rolls out.
func forEachPeerCacheMode(t *testing.T, f func(t *testing.T, senderCompact, receiverCompact bool)) {
	t.Helper()
	forEachCacheMode(t, func(t *testing.T, senderCompact bool) {
		forEachCacheMode(t, func(t *testing.T, receiverCompact bool) { f(t, senderCompact, receiverCompact) })
	})
}

// cachedAs returns u in the representation the informer cache holds for the mode.
func cachedAs(t *testing.T, compact bool, u *unstructured.Unstructured) resourceObject {
	t.Helper()
	if !compact {
		return u
	}
	c, err := newCompactObject(u)
	require.NoError(t, err)
	return c
}

func cachedAll(t *testing.T, compact bool, us ...*unstructured.Unstructured) []resourceObject {
	t.Helper()
	out := make([]resourceObject, 0, len(us))
	for _, u := range us {
		out = append(out, cachedAs(t, compact, u))
	}
	return out
}

func mustDecode(t *testing.T, obj resourceObject) *unstructured.Unstructured {
	t.Helper()
	u, err := decodeObject(obj)
	require.NoError(t, err)
	return u
}

func assertRepresentation(t *testing.T, compact bool, obj resourceObject) {
	t.Helper()
	_, isCompact := obj.(*compactObject)
	assert.Equal(t, compact, isCompact, "unexpected cached representation %T", obj)
}

// fromAPI decodes an object the way the dynamic client does.
func fromAPI(t *testing.T, data string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	require.NoError(t, u.UnmarshalJSON([]byte(data)))
	return u
}

const compactFixtureWidget = `{
	"apiVersion": "example.com/v1", "kind": "Widget",
	"metadata": {"name": "w", "namespace": "prod", "uid": "u-1", "resourceVersion": "7",
		"labels": {"app": "<a&b>"},
		"managedFields": [{"manager": "kubectl"}]},
	"spec": {"replicas": 3, "ratio": 0.25, "big": 9007199254740993, "huge": 1e300,
		"items": [1, "two", {"three": null}, [true, false]], "empty": {}, "none": [], "text": "héllo"}
}`

func TestCompactObject_KeepsMetadataAndDecodesToTheLiveObject(t *testing.T) {
	transform := sanitizeObject(defaultConfigMapMaxDataSize)
	live, err := transform(fromAPI(t, compactFixtureWidget))
	require.NoError(t, err)
	compact, err := compactTransform(transform)(fromAPI(t, compactFixtureWidget))
	require.NoError(t, err)

	liveObj, ok := live.(*unstructured.Unstructured)
	require.True(t, ok)
	c, ok := compact.(*compactObject)
	require.True(t, ok)

	assert.Equal(t, "w", c.GetName())
	assert.Equal(t, "prod", c.GetNamespace())
	assert.Equal(t, "7", c.GetResourceVersion())
	assert.Equal(t, "Widget", c.GetKind())
	assert.Equal(t, liveObj.GetUID(), c.GetUID())
	assert.Equal(t, liveObj.Object, mustDecode(t, c).Object)

	liveSize, ok := objectPayloadSize(liveObj)
	require.True(t, ok)
	compactSize, ok := objectPayloadSize(c)
	require.True(t, ok)
	assert.Equal(t, liveSize, compactSize)

	again, err := compactTransform(transform)(c)
	require.NoError(t, err)
	assert.Same(t, c, again, "transform must be idempotent for compact entries")

	copied, ok := c.DeepCopyObject().(*compactObject)
	require.True(t, ok)
	assert.Equal(t, c, copied)
	assert.NotSame(t, c, copied)
}

func TestCompactObject_WireFormatMatchesLive(t *testing.T) {
	live := fromAPI(t, compactFixtureWidget)
	c := cachedAs(t, true, live)

	liveRaw, err := marshalWireObject(live)
	require.NoError(t, err)
	compactRaw, err := marshalWireObject(c)
	require.NoError(t, err)
	assert.JSONEq(t, string(liveRaw), string(compactRaw))

	forEachCacheMode(t, func(t *testing.T, compact bool) {
		got, err := unmarshalWireObject(compactRaw, compact)
		require.NoError(t, err)
		assertRepresentation(t, compact, got)
		assert.Equal(t, "w", got.GetName())
		assert.Equal(t, "7", got.GetResourceVersion())
		assert.Equal(t, live.Object, mustDecode(t, got).Object)
	})
}

func TestCompactObject_InformerKeyingAndTombstones(t *testing.T) {
	pod := func(name, rv string) *unstructured.Unstructured {
		u := makeTestPod(name, "default", nil)
		u.SetResourceVersion(rv)
		return u
	}
	transform := compactTransform(sanitizeObject(0))

	compactA, err := transform(pod("a", "1"))
	require.NoError(t, err)
	liveKey, err := cache.MetaNamespaceKeyFunc(pod("a", "1"))
	require.NoError(t, err)
	compactKey, err := cache.MetaNamespaceKeyFunc(compactA)
	require.NoError(t, err)
	assert.Equal(t, liveKey, compactKey)
	tombstoneKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(
		cache.DeletedFinalStateUnknown{Key: compactKey, Obj: compactA})
	require.NoError(t, err)
	assert.Equal(t, compactKey, tombstoneKey)

	// A relist that no longer returns "a" tombstones the cached compact entry.
	indexer := cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(compactA))
	compactB, err := transform(pod("b", "1"))
	require.NoError(t, err)
	require.NoError(t, indexer.Add(compactB))

	fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
		KnownObjects:          indexer,
		Transformer:           transform,
		EmitDeltaTypeReplaced: true,
	})
	require.NoError(t, fifo.Replace([]interface{}{pod("b", "2")}, "2"))

	var tombstones int
	for range 2 {
		_, err := fifo.Pop(func(obj interface{}, _ bool) error {
			deltas, ok := obj.(cache.Deltas)
			require.True(t, ok)
			for _, d := range deltas {
				if d.Type == cache.Deleted {
					tombstone, ok := d.Object.(cache.DeletedFinalStateUnknown)
					require.True(t, ok)
					assert.Same(t, compactA, tombstone.Obj)
					tombstones++
					require.NoError(t, indexer.Delete(d.Object))
					continue
				}
				obj, ok := toResourceObject(d.Object)
				require.True(t, ok)
				assertRepresentation(t, true, obj)
				require.NoError(t, indexer.Update(d.Object))
			}
			return nil
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 1, tombstones)
	assert.Equal(t, []string{"default/b"}, indexer.ListKeys())
	stored, ok := indexer.List()[0].(*compactObject)
	require.True(t, ok)
	assert.Equal(t, "2", stored.GetResourceVersion())
}

// fixedInformers serves preset objects to the collector.
type fixedInformers struct {
	crds    []*unstructured.Unstructured
	objects map[schema.GroupVersionResource]ObjectGroup
}

func (f *fixedInformers) ReadCRDs() []*unstructured.Unstructured { return f.crds }
func (f *fixedInformers) ReadObjects() map[schema.GroupVersionResource]ObjectGroup {
	return f.objects
}
func (f *fixedInformers) StaticWatchesSynced() bool        { return true }
func (f *fixedInformers) Start(_ context.Context) error    { return nil }
func (f *fixedInformers) Shutdown(_ context.Context) error { return nil }

type objectFixture struct {
	gvr    schema.GroupVersionResource
	source ObjectSource
	json   string
}

type emittedRecord struct {
	Resource map[string]any
	Event    string
	Attrs    map[string]any
	Body     map[string]any
}

func emittedRecords(sink *consumertest.LogsSink, from int) []emittedRecord {
	var out []emittedRecord
	for _, logs := range sink.AllLogs()[from:] {
		for i := 0; i < logs.ResourceLogs().Len(); i++ {
			rl := logs.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				records := rl.ScopeLogs().At(j).LogRecords()
				for k := 0; k < records.Len(); k++ {
					r := records.At(k)
					out = append(out, emittedRecord{
						Resource: rl.Resource().Attributes().AsRaw(),
						Event:    r.EventName(),
						Attrs:    r.Attributes().AsRaw(),
						Body:     r.Body().Map().AsRaw(),
					})
				}
			}
		}
	}
	return out
}

// runCompactCacheCycles drives the collector through a snapshot, an increment
// with add/modify/delete, and a snapshot carrying a delete, returning each
// cycle's records and the recorded payload sizes.
func runCompactCacheCycles(t *testing.T, compact bool) ([][]emittedRecord, []payloadSizeRecord) {
	t.Helper()
	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	secretsGVR := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	configMapsGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	widgetsGVR := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: testWidgetsResource}

	podJSON := func(rv string, restarts int) string {
		return `{"apiVersion": "v1", "kind": "Pod",
			"metadata": {"name": "a", "namespace": "default", "resourceVersion": "` + rv + `",
				"managedFields": [{"manager": "kubelet"}]},
			"status": {"phase": "Running", "containerStatuses": [
				{"name": "app", "ready": true, "restartCount": ` + strconv.Itoa(restarts) + `},
				{"name": "sidecar", "ready": false, "restartCount": 0}]}}`
	}
	podA1 := objectFixture{podsGVR, ObjectSourceStatic, podJSON("1", 1)}
	podA2 := objectFixture{podsGVR, ObjectSourceStatic, podJSON("2", 4)}
	secretB := objectFixture{secretsGVR, ObjectSourceStatic, `{"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": {"name": "b", "namespace": "default", "resourceVersion": "1",
			"annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{}"}},
		"data": {"password": "aHVudGVyMg=="}}`}
	configMapC := objectFixture{configMapsGVR, ObjectSourceStatic, `{"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": {"name": "c", "namespace": "default", "resourceVersion": "1"},
		"data": {"config.yaml": "key: value"}, "binaryData": {"blob": "AAEC"}}`}
	widgetD := objectFixture{widgetsGVR, ObjectSourceCR, compactFixtureWidget}
	widgetE := objectFixture{widgetsGVR, ObjectSourceCR, `{"apiVersion": "example.com/v1", "kind": "Widget",
		"metadata": {"name": "e", "resourceVersion": "3"}, "spec": {"weight": 1.5}}`}

	transform := sanitizeObject(defaultConfigMapMaxDataSize)
	if compact {
		transform = compactTransform(transform)
	}
	read := func(fixtures ...objectFixture) map[schema.GroupVersionResource]ObjectGroup {
		out := map[schema.GroupVersionResource]ObjectGroup{}
		for _, f := range fixtures {
			obj, err := transform(fromAPI(t, f.json))
			require.NoError(t, err)
			group := out[f.gvr]
			group.Source = f.source
			group.Objects = append(group.Objects, obj.(resourceObject)) //nolint:forcetypeassert
			out[f.gvr] = group
		}
		return out
	}

	cfg := testConfig([]string{testExampleGroup}, nil)
	cfg.ClusterName = "test-cluster"
	cfg.MaxCRTotalDataSizeBytes = 1 << 20
	cfg.MaxObjectTotalDataSizeBytes = 1 << 20
	crd := makeTestCRDUnstructured("widgets.example.com", testExampleGroup, "Widget", testWidgetsResource)
	crd.SetResourceVersion("1")
	informers := &fixedInformers{crds: []*unstructured.Unstructured{crd}}
	store := newPeerSyncCacheStore(zaptest.NewLogger(t), 0, "", nil)
	store.compactObjects = compact
	sink := &consumertest.LogsSink{}
	recorder := &payloadRecorder{}
	c := newResourceCollector(zaptest.NewLogger(t), cfg, sink, informers, store, recorder, nil)
	ctx := context.Background()

	var cycles [][]emittedRecord
	runCycle := func(objects map[schema.GroupVersionResource]ObjectGroup) {
		from := len(sink.AllLogs())
		informers.objects = objects
		c.runIncrement(ctx)
		cycles = append(cycles, emittedRecords(sink, from))
	}

	runCycle(read(podA1, secretB, configMapC, widgetD))
	runCycle(read(podA2, configMapC, widgetD, widgetE))
	c.lastSnapshotTime = time.Time{}
	runCycle(read(podA2, configMapC, widgetE))

	store.cacheMu.RLock()
	for _, obj := range store.cache.Objects {
		assertRepresentation(t, compact, obj.Obj)
	}
	store.cacheMu.RUnlock()
	return cycles, recorder.Sizes()
}

func TestCompactCache_EmitsIdenticalLogRecords(t *testing.T) {
	liveCycles, liveSizes := runCompactCacheCycles(t, false)
	compactCycles, compactSizes := runCompactCacheCycles(t, true)

	require.Len(t, liveCycles, 3)
	assert.Len(t, liveCycles[0], 5, "snapshot: CRD and four objects")
	assert.Len(t, liveCycles[1], 3, "increment: modify, add, delete")
	assert.Len(t, liveCycles[2], 5, "snapshot: CRD, three objects and a delete")
	require.Len(t, compactCycles, len(liveCycles))
	for i := range liveCycles {
		assert.ElementsMatch(t, liveCycles[i], compactCycles[i], "cycle %d", i)
	}
	assert.ElementsMatch(t, liveSizes, compactSizes)
}

func TestResourceCollector_SkipsCompactObjectThatFailsToDecode(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testExampleGroup, Version: "v1", Resource: testWidgetsResource}
	good := cachedAs(t, true, makeTestCR("good", "default", testExampleGroup, "v1", "Widget"))
	broken := &compactObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: "example.com/v1", Kind: "Widget"},
		ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "default"},
		raw:        []byte(`{"truncated":`),
	}
	core, logs := observer.New(zap.WarnLevel)
	sink := &consumertest.LogsSink{}
	c := &resourceCollector{
		logger:   zap.New(core),
		config:   &Config{ClusterName: "test-cluster"},
		consumer: sink,
		metrics:  metrics.NoopRecorder{},
		enricher: noopResourceAttributeEnricher{},
	}

	c.emitSnapshot(context.Background(), nil,
		map[schema.GroupVersionResource]ObjectGroup{gvr: {Source: ObjectSourceCR, Objects: []resourceObject{broken, good}}},
		[]ResourceChange{{Obj: broken, EventType: watch.Deleted, GVR: gvr, Source: ObjectSourceCR}},
	)
	c.emitChanges(context.Background(), []ResourceChange{
		{Obj: broken, EventType: watch.Modified, GVR: gvr, Source: ObjectSourceCR},
		{Obj: good, EventType: watch.Modified, GVR: gvr, Source: ObjectSourceCR},
	})

	assertEventNamesByObjectName(t, sink, map[string]string{"good": emit.EventNameCR})
	assert.Equal(t, 2, sink.LogRecordCount())
	warnings := logs.FilterField(zap.String("name", "broken")).All()
	require.Len(t, warnings, 3, "one warning per failed emission")
	for _, w := range warnings {
		assert.Equal(t, zap.WarnLevel, w.Level)
	}
}

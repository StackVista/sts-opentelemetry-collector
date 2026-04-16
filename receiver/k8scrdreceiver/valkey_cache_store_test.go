//nolint:testpackage // Tests require access to internal types
package k8scrdreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestValkeyCacheStore_Save(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)
	store := newValkeyCacheStore(logger, client, "test-cluster")

	cache := newResourceCache()
	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")
	crd.SetResourceVersion("5")
	cache.crds["widgets.example.com"] = crd

	client.EXPECT().
		Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
			return len(cmd) >= 2 && cmd[0] == "SET" && cmd[1] == "k8scrdreceiver:cache:test-cluster"
		}, "SET k8scrdreceiver:cache:test-cluster <value>")).
		Return(mock.Result(mock.ValkeyString("OK")))

	err := store.Save(context.Background(), cache)
	require.NoError(t, err)
}

func TestValkeyCacheStore_LoadWithData(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)
	store := newValkeyCacheStore(logger, client, "test-cluster")

	// Build a cache, serialize it, and set up the mock to return it
	original := newResourceCache()
	crd := makeTestCRDUnstructured("widgets.example.com", "example.com", "Widget", "widgets")
	crd.SetResourceVersion("5")
	original.crds["widgets.example.com"] = crd

	gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	cr := makeTestCR("my-widget", "default", "example.com", "v1", "Widget")
	cr.SetResourceVersion("10")
	key := crResourceKey(gvr, "default", "my-widget")
	original.crs[key] = &cachedCR{obj: cr, gvr: gvr}

	data, err := marshalResourceCache(original)
	require.NoError(t, err)

	client.EXPECT().
		Do(gomock.Any(), mock.Match("GET", "k8scrdreceiver:cache:test-cluster")).
		Return(mock.Result(mock.ValkeyBlobString(string(data))))

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)

	require.Len(t, loaded.crds, 1)
	assert.Equal(t, "widgets.example.com", loaded.crds["widgets.example.com"].GetName())
	assert.Equal(t, "5", loaded.crds["widgets.example.com"].GetResourceVersion())

	require.Len(t, loaded.crs, 1)
	loadedCR := loaded.crs[key]
	require.NotNil(t, loadedCR)
	assert.Equal(t, "my-widget", loadedCR.obj.GetName())
	assert.Equal(t, "default", loadedCR.obj.GetNamespace())
	assert.Equal(t, gvr, loadedCR.gvr)
}

func TestValkeyCacheStore_LoadReturnsEmptyCacheOnNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)
	store := newValkeyCacheStore(logger, client, "test-cluster")

	// Return a nil response (key doesn't exist)
	client.EXPECT().
		Do(gomock.Any(), mock.Match("GET", "k8scrdreceiver:cache:test-cluster")).
		Return(mock.Result(mock.ValkeyNil()))

	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, loaded.isEmpty())
}

func TestValkeyCacheStore_LoadReturnsErrorOnValkeyFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)
	store := newValkeyCacheStore(logger, client, "test-cluster")

	client.EXPECT().
		Do(gomock.Any(), mock.Match("GET", "k8scrdreceiver:cache:test-cluster")).
		Return(mock.ErrorResult(assert.AnError))

	_, err := store.Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey GET")
}

func TestValkeyCacheStore_SaveReturnsErrorOnValkeyFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)
	store := newValkeyCacheStore(logger, client, "test-cluster")

	client.EXPECT().
		Do(gomock.Any(), mock.MatchFn(func(cmd []string) bool {
			return len(cmd) >= 2 && cmd[0] == "SET" && cmd[1] == "k8scrdreceiver:cache:test-cluster"
		}, "SET k8scrdreceiver:cache:test-cluster <value>")).
		Return(mock.ErrorResult(assert.AnError))

	err := store.Save(context.Background(), newResourceCache())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey SET")
}

func TestValkeyCacheStore_KeyIncludesClusterName(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	logger := zaptest.NewLogger(t)

	store := newValkeyCacheStore(logger, client, "production")
	assert.Equal(t, "k8scrdreceiver:cache:production", store.key)

	store2 := newValkeyCacheStore(logger, client, "staging")
	assert.Equal(t, "k8scrdreceiver:cache:staging", store2.key)
}

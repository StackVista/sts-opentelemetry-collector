package k8scrdreceiver

import (
	"context"
	"fmt"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	// defaultCacheKey is the Valkey key used to store the serialized resource cache.
	// The cluster name is appended to namespace caches per cluster.
	cacheKeyPrefix = "k8scrdreceiver:cache:"
)

// valkeyCacheStore persists the resource cache to a Valkey instance.
type valkeyCacheStore struct {
	client valkey.Client
	key    string
	logger *zap.Logger
}

var _ CacheStore = (*valkeyCacheStore)(nil)

func newValkeyCacheStore(logger *zap.Logger, client valkey.Client, clusterName string) *valkeyCacheStore {
	key := cacheKeyPrefix + clusterName
	return &valkeyCacheStore{
		client: client,
		key:    key,
		logger: logger,
	}
}

func (v *valkeyCacheStore) Load(ctx context.Context) (*resourceCache, error) {
	cmd := v.client.B().Get().Key(v.key).Build()
	resp := v.client.Do(ctx, cmd)

	if err := resp.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			v.logger.Debug("No cached state found in Valkey, starting fresh", zap.String("key", v.key))
			return newResourceCache(), nil
		}
		return nil, fmt.Errorf("valkey GET %s: %w", v.key, err)
	}

	data, err := resp.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("valkey GET %s read bytes: %w", v.key, err)
	}

	cache, err := unmarshalResourceCache(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal cache from valkey: %w", err)
	}

	v.logger.Debug("Loaded cache from Valkey",
		zap.String("key", v.key),
		zap.Int("crds", len(cache.crds)),
		zap.Int("crs", len(cache.crs)),
	)

	return cache, nil
}

func (v *valkeyCacheStore) Save(ctx context.Context, cache *resourceCache) error {
	data, err := marshalResourceCache(cache)
	if err != nil {
		return fmt.Errorf("marshal cache for valkey: %w", err)
	}

	cmd := v.client.B().Set().Key(v.key).Value(string(data)).Build()
	if err := v.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey SET %s: %w", v.key, err)
	}

	v.logger.Debug("Saved cache to Valkey",
		zap.String("key", v.key),
		zap.Int("bytes", len(data)),
		zap.Int("crds", len(cache.crds)),
		zap.Int("crs", len(cache.crs)),
	)

	return nil
}

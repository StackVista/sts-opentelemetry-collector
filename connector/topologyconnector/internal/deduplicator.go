package internal

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/types"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.uber.org/zap"
)

//nolint:gochecknoglobals
var separator = []byte{0}

type Deduplicator interface {
	ShouldSend(
		mappingIdentifier string,
		signal settings.OtelInputSignal,
		evalCtx *ExpressionEvalContext,
		expressionRef *types.ExpressionRefSummary,
		ttl time.Duration,
	) bool
}

type NoopDeduplicator struct{}

func NewNoopDeduplicator() *NoopDeduplicator {
	return &NoopDeduplicator{}
}

func (n *NoopDeduplicator) ShouldSend(
	_ string,
	_ settings.OtelInputSignal,
	_ *ExpressionEvalContext,
	_ *types.ExpressionRefSummary,
	_ time.Duration,
) bool {
	return true
}

type TopologyDeduplicator struct {
	logger *zap.Logger

	enabled         bool
	refreshFraction float64
	cacheTTL        time.Duration

	hasher DedupHasher

	cache *metrics.MeteredCache[string, dedupEntry]
}

type dedupEntry struct {
	lastSent time.Time
}

type DeduplicationConfig struct {
	Enabled         bool
	RefreshFraction float64
	CacheConfig     metrics.MeteredCacheSettings
}

func NewTopologyDeduplicator(
	ctx context.Context,
	logger *zap.Logger,
	dedupCfg DeduplicationConfig,
) *TopologyDeduplicator {
	cache := metrics.NewCache[string, dedupEntry](ctx, dedupCfg.CacheConfig, nil)

	// same as validation done for DeduplicationSettings.RefreshFraction in config.go
	normalisedRefreshFraction := dedupCfg.RefreshFraction
	if normalisedRefreshFraction < 0 {
		normalisedRefreshFraction = 0.5
	}
	if normalisedRefreshFraction > 1 {
		normalisedRefreshFraction = 1
	}

	return &TopologyDeduplicator{
		logger:          logger,
		enabled:         dedupCfg.Enabled,
		refreshFraction: normalisedRefreshFraction,
		cacheTTL:        dedupCfg.CacheConfig.TTL,
		hasher:          &SignalMappingProjectionHasher{},
		cache:           cache,
	}
}

// ShouldSend determines whether the current mapping invocation should emit output
// downstream, based on TTL-aware deduplication.
//
// The decision is made by computing a deterministic projection hash over the subset
// of signal data that can influence the mapping output (as described by
// ExpressionRefSummary). This projection hash serves as both the deduplication key
// and the content identifier.
//
// Note: An ExpressionRefSummary can be in one of three states:
//   - nil: No deduplication is performed for this mapping.
//   - empty: A valid ExpressionRefSummary with no expression references. Only resource and scope data will be
//     included in deduplication.
//   - populated: A valid ExpressionRefSummary with expression references for datapoint, span, or metric attributes.
//     Resource and scope data will still be included, along with the specific fields referenced by the expressions.
//
// Semantics:
//
//   - A "series" is defined by the projection hash. Any change in the projected
//     inputs results in a different hash and is treated as a new series.
//
//   - If no entry exists for the computed key, the mapping is considered new and
//     the result must be sent.
//
//   - If an entry exists and the projected inputs have not changed, the result is
//     only sent if enough time has passed to satisfy the refresh policy.
//
// TTL handling:
//
//   - mappingTTL controls how often a mapping must be refreshed to prevent the
//     corresponding topology entities from expiring downstream.
//
//   - The refresh interval is derived as:
//     refreshInterval = mappingTTL * refreshFraction
//
//   - If the time since the last successful send for this key is greater than or
//     equal to the refresh interval, the result is sent again even if the content
//     is unchanged.
//
// Cache behavior:
//
//   - The underlying cache TTL (configured via MeteredCacheSettings.TTL) controls
//     how long deduplication state is retained in memory and is independent of
//     mappingTTL.
//
//   - The cache TTL should be configured to be greater than or equal to the maximum
//     mappingTTL to avoid premature eviction of active entries.
//
//   - Cache eviction only affects memory usage; it does not change the semantic
//     correctness of deduplication. Evicted entries will be treated as new on the
//     next invocation.
func (d *TopologyDeduplicator) ShouldSend(
	mappingIdentifier string,
	signal settings.OtelInputSignal,
	evalCtx *ExpressionEvalContext,
	expressionRef *types.ExpressionRefSummary,
	mappingTTL time.Duration,
) bool {
	if !d.enabled || expressionRef == nil {
		return true
	}

	now := time.Now()
	refreshInterval := time.Duration(float64(mappingTTL) * d.refreshFraction)

	if d.cacheTTL > 0 && d.cacheTTL < refreshInterval { // cacheTTL == 0 means no eviction
		d.logger.Info(
			"Deduplication cache TTL may be too small to cover the effective deduplication window; "+
				"mapping might not benefit from deduplication",
			zap.String("mappingIdentifier", mappingIdentifier),
			zap.Duration("cacheTTL", d.cacheTTL),
			zap.Duration("mappingTTL", mappingTTL),
			zap.Duration("effectiveDedupWindow", refreshInterval),
		)
	}

	// Generate deterministic key from the projection of signal inputs.
	// This serves as both the cache key and content identifier.
	// Any change in the projection results in a different key, naturally
	// creating a new cache entry for what is effectively a different series.
	key := d.hasher.ProjectionHash(mappingIdentifier, signal, evalCtx, expressionRef)

	d.logger.Debug(
		"Deduplication key", zap.String("mappingIdentifier", mappingIdentifier), zap.String("key", key),
	)

	e, ok := d.cache.Get(key)
	if !ok {
		// New series (or content changed creating new key) -> send and record
		d.cache.Add(key, dedupEntry{lastSent: now})
		return true
	}

	// Existing series with same content -> only refresh according to time policy
	if now.Sub(e.lastSent) >= refreshInterval {
		d.cache.Add(key, dedupEntry{lastSent: now})
		return true
	}

	// Skip sending - content unchanged and not time to refresh
	return false
}

type DedupHasher interface {
	// ProjectionHash generates a stable identifier based on structure, not content
	ProjectionHash(
		mappingIdentifier string,
		signal settings.OtelInputSignal,
		ctx *ExpressionEvalContext,
		ref *types.ExpressionRefSummary,
	) string
}

type SignalMappingProjectionHasher struct{}

// ProjectionHash computes a stable hash for the subset of signal data to deduplicate on.
// It always includes the full resource map and only the referenced vars/attributes for the rest.
// If ref is nil, fall back to hashing only mapping+signal to avoid false positives.
func (s *SignalMappingProjectionHasher) ProjectionHash(
	mappingIdentifier string,
	signal settings.OtelInputSignal,
	ctx *ExpressionEvalContext,
	ref *types.ExpressionRefSummary,
) string {
	h := xxhash.New()

	mustWrite(h, []byte(mappingIdentifier))
	mustWrite(h, separator)
	mustWrite(h, []byte(signal))
	mustWrite(h, separator)

	// Resource (always include full resource)
	if ctx.Resource != nil {
		mustWrite(h, []byte{'R'})
		canonicalHashValue(h, ctx.Resource.ToMap())
	}
	mustWrite(h, separator)

	// Scope (always include full scope)
	if ctx.Scope != nil {
		mustWrite(h, []byte{'C'})
		canonicalHashValue(h, ctx.Scope.ToMap())
	}
	mustWrite(h, separator)

	// Datapoint attributes (only referenced)
	if ctx.Datapoint != nil && ref != nil && ref.Datapoint.HasRefs() {
		hashSelectedEntityFields(h, ctx.Datapoint.ToMap(), ref.Datapoint, 'D')
	}
	mustWrite(h, separator)

	// Span attributes (only referenced)
	if ctx.Span != nil && ref != nil && ref.Span.HasRefs() {
		hashSelectedEntityFields(h, ctx.Span.ToMap(), ref.Span, 'P')
	}
	mustWrite(h, separator)

	// Metric attributes (only referenced)
	if ctx.Metric != nil && ref != nil && ref.Metric.HasRefs() {
		hashSelectedEntityFields(h, ctx.Metric.ToMap(), ref.Metric, 'M')
	}
	mustWrite(h, separator)

	return hex.EncodeToString(h.Sum(nil))
}

// canonicalHashValue walks v and writes a deterministic representation into hasher.
// Note: xxhash.Write() never returns an error, so we can safely ignore it.
func canonicalHashValue(h io.Writer, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mustWrite(h, []byte{'K'})
			mustWrite(h, []byte(k))
			mustWrite(h, []byte{'V'})
			canonicalHashValue(h, t[k])
			mustWrite(h, []byte{';'})
		}
	case []any:
		for _, it := range t {
			mustWrite(h, []byte{'['})
			canonicalHashValue(h, it)
			mustWrite(h, []byte{']'})
		}
	case string:
		mustWrite(h, []byte{'S'})
		mustWrite(h, []byte(t))
	case []byte:
		mustWrite(h, []byte{'B'})
		mustWrite(h, t)
	case bool:
		if t {
			mustWrite(h, []byte{'T'})
		} else {
			mustWrite(h, []byte{'F'})
		}
	case int:
		canonicalHashValue(h, int64(t))
	case int32:
		canonicalHashValue(h, int64(t))
	case int64:
		mustWrite(h, []byte{'I'})
		writeInt64(h, t)
	case uint:
		canonicalHashValue(h, uint64(t))
	case uint32:
		canonicalHashValue(h, uint64(t))
	case uint64:
		mustWrite(h, []byte{'U'})
		writeUint64(h, t)
	case float32:
		canonicalHashValue(h, float64(t))
	case float64:
		// Use IEEE 754 bits for determinism
		bits := math.Float64bits(t)
		canonicalHashValue(h, bits)
	case nil:
		mustWrite(h, []byte{'N'})
	default:
		// Fallback for unexpected types
		mustWrite(h, []byte{'X'})
		mustWrite(h, []byte(fmt.Sprintf("%v", t)))
	}
}

func writeInt64(w io.Writer, n int64) {
	var buf [8]byte
	//nolint:gosec
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	_, _ = w.Write(buf[:])
}

func writeUint64(w io.Writer, n uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], n)
	_, _ = w.Write(buf[:])
}

// hashSelectedEntityFields hashes selected fields from an entity map deterministically.
// Fields can be top-level (e.g., span.name, metric.unit) or nested under "attributes".
// The prefix helps distinguish field types in the hash (e.g., "span:name" vs "metric:name").
// Note: xxhash.Write() never returns an error, so we can safely ignore it.
func hashSelectedEntityFields(
	w io.Writer, entity map[string]any, entityRefSummary types.EntityRefSummary, prefix byte,
) {
	// Full attribute map
	if entityRefSummary.AllAttributes {
		if attrsRaw, ok := entity["attributes"]; ok {
			if attrs, typeOk := attrsRaw.(map[string]any); typeOk {
				mustWrite(w, []byte{prefix, '*'})
				canonicalHashValue(w, attrs)
				mustWrite(w, []byte{';'})
			}
		}
	}

	// Top-level fields
	for _, f := range entityRefSummary.FieldKeys {
		val, exists := entity[f]
		if !exists {
			continue
		}

		mustWrite(w, []byte{prefix, '.'})
		mustWrite(w, []byte(f))
		mustWrite(w, []byte{'='})
		canonicalHashValue(w, val)
		mustWrite(w, []byte{';'})
	}

	// Only hash individual attribute keys if AllAttributes is false
	if !entityRefSummary.AllAttributes {
		attrsRaw, ok := entity["attributes"]
		if !ok {
			return
		}
		attrs, ok := attrsRaw.(map[string]any)
		if !ok {
			return
		}

		for _, k := range entityRefSummary.AttributeKeys {
			val, exists := attrs[k]
			if !exists {
				continue
			}

			mustWrite(w, []byte{prefix, '@'})
			mustWrite(w, []byte(k))
			mustWrite(w, []byte{'='})
			canonicalHashValue(w, val)
			mustWrite(w, []byte{';'})
		}
	}
}

// mustWrite writes b to w and panics on error.
//
// Note: xxhash.Write() never returns an error, so we can safely ignore it. Also, doing it this way avoids lint errors.
func mustWrite(x io.Writer, b []byte) {
	//nolint:errcheck
	x.Write(b)
}

package internal

import (
	"context"
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
// Note: Deduplication is only applied when selective expression references are known.
// If expressionRef is nil, the mapping depends only on resource/scope and deduplication
// is intentionally skipped to avoid over-deduplication.
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

	if d.cacheTTL > 0 && d.cacheTTL < mappingTTL { // cacheTTL == 0 means no eviction
		d.logger.Warn(
			"Dedup cache TTL is smaller than mapping TTL; entries may be evicted prematurely.",
			zap.Duration("cacheTTL", d.cacheTTL),
			zap.String("mappingIdentifier", mappingIdentifier),
			zap.Duration("mappingTTL", mappingTTL),
		)
	}

	now := time.Now()
	refreshBefore := time.Duration(float64(mappingTTL) * d.refreshFraction)

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
	if now.Sub(e.lastSent) >= refreshBefore {
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

	// Domain separator and identifiers
	mustWrite(h, []byte("projection_v1\x00"))
	mustWrite(h, []byte(mappingIdentifier))
	mustWrite(h, separator)
	mustWrite(h, []byte(signal))
	mustWrite(h, separator)

	// Resource (always include full resource)
	if ctx.Resource != nil {
		mustWrite(h, []byte("resource:"))
		canonicalHashValue(h, ctx.Resource.ToMap())
	}
	mustWrite(h, separator)

	// Scope (always include full scope)
	if ctx.Scope != nil {
		mustWrite(h, []byte("scope:"))
		canonicalHashValue(h, ctx.Scope.ToMap())
	}
	mustWrite(h, separator)

	// Vars (only referenced ones)
	if ctx.Vars != nil && ref != nil && len(ref.Vars()) > 0 {
		for _, k := range ref.Vars() {
			mustWrite(h, []byte("var:"))
			mustWrite(h, []byte(k))
			mustWrite(h, []byte{'='})
			canonicalHashValue(h, ctx.Vars[k])
			mustWrite(h, []byte{';'})
		}
	}
	mustWrite(h, separator)

	// Datapoint attributes (only referenced)
	if ctx.Datapoint != nil && ref != nil && len(ref.DatapointKeys()) > 0 {
		hashSelectedEntityFields(h, ctx.Datapoint.ToMap(), ref.DatapointKeys(), "dp")
	}
	mustWrite(h, separator)

	// Span attributes (only referenced)
	if ctx.Span != nil && ref != nil && len(ref.SpanKeys()) > 0 {
		hashSelectedEntityFields(h, ctx.Span.ToMap(), ref.SpanKeys(), "span")
	}
	mustWrite(h, separator)

	// Metric attributes (only referenced)
	if ctx.Metric != nil && ref != nil && len(ref.MetricKeys()) > 0 {
		hashSelectedEntityFields(h, ctx.Metric.ToMap(), ref.MetricKeys(), "metric")
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
			mustWrite(h, []byte("k:"))
			mustWrite(h, []byte(k))
			mustWrite(h, []byte(";v:"))
			canonicalHashValue(h, t[k])
			mustWrite(h, []byte(";"))
		}
	case []any:
		for _, it := range t {
			mustWrite(h, []byte("["))
			canonicalHashValue(h, it)
			mustWrite(h, []byte("]"))
		}
	case string:
		mustWrite(h, []byte("s:"))
		mustWrite(h, []byte(t))
	case []byte:
		mustWrite(h, []byte("b:"))
		mustWrite(h, t)
	case bool:
		if t {
			mustWrite(h, []byte("t"))
		} else {
			mustWrite(h, []byte("f"))
		}
	case int:
		canonicalHashValue(h, int64(t))
	case int32:
		canonicalHashValue(h, int64(t))
	case int64:
		mustWrite(h, []byte("i:"))
		writeInt64(h, t)
	case uint:
		canonicalHashValue(h, uint64(t))
	case uint32:
		canonicalHashValue(h, uint64(t))
	case uint64:
		mustWrite(h, []byte("u:"))
		writeUint64(h, t)
	case float32:
		canonicalHashValue(h, float64(t))
	case float64:
		// Use IEEE 754 bits for determinism
		bits := math.Float64bits(t)
		canonicalHashValue(h, bits)
	case nil:
		mustWrite(h, []byte("nil"))
	default:
		// Fallback for unexpected types
		mustWrite(h, []byte("x:"))
		mustWrite(h, []byte(fmt.Sprintf("%v", t)))
	}
}

// writeInt64 writes an int64 as decimal ASCII (without using fmt, which would allocate memory)
func writeInt64(w io.Writer, n int64) {
	if n == 0 {
		mustWrite(w, []byte{'0'})
		return
	}

	var u uint64
	if n < 0 {
		mustWrite(w, []byte{'-'})
		// Handle MinInt64 correctly - int64: -9223372036854775808 to 9223372036854775807
		//nolint:gosec
		u = uint64(-(n + 1)) + 1
	} else {
		u = uint64(n)
	}
	writeUint64(w, u)
}

// writeUint64 writes a uint64 as decimal ASCII (without using fmt, which would allocate memory)
func writeUint64(w io.Writer, n uint64) {
	if n == 0 {
		mustWrite(w, []byte{'0'})
		return
	}

	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10) // `+ digit` converts digit to ASCII
		n /= 10
	}
	mustWrite(w, buf[i:])
}

// hashSelectedEntityFields hashes selected fields from an entity map deterministically.
// Fields can be top-level (e.g., span.name, metric.unit) or nested under "attributes".
// The prefix helps distinguish field types in the hash (e.g., "span:name" vs "metric:name").
// Note: xxhash.Write() never returns an error, so we can safely ignore it.
func hashSelectedEntityFields(w io.Writer, entity map[string]any, keys []string, prefix string) {
	for _, k := range keys { // keys are sorted
		// Try top-level field first
		val, exists := entity[k]

		// If not found, try under "attributes" (for span/datapoint)
		if !exists {
			if attrsRaw, ok := entity["attributes"]; ok {
				if attrs, ok := attrsRaw.(map[string]any); ok {
					val, exists = attrs[k]
				}
			}
		}

		// Skip if field doesn't exist anywhere
		if !exists {
			continue
		}

		// Hash the field with prefix for domain separation
		mustWrite(w, []byte(prefix))
		mustWrite(w, []byte(":"))
		mustWrite(w, []byte(k))
		mustWrite(w, []byte("="))
		canonicalHashValue(w, val)
		mustWrite(w, []byte(";"))
	}
}

// mustWrite writes b to w and panics on error.
//
// Note: xxhash.Write() never returns an error, so we can safely ignore it. Also, doing it this way avoids lint errors.
func mustWrite(x io.Writer, b []byte) {
	//nolint:errcheck
	x.Write(b)
}

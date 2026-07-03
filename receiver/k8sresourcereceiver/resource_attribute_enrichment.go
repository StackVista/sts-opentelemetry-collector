package k8sresourcereceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/emit"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/internal/metrics"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

type resourceAttributeEnricher interface {
	AttributesFor(gvr schema.GroupVersionResource) map[string]string
}

type noopResourceAttributeEnricher struct{}

func (noopResourceAttributeEnricher) AttributesFor(schema.GroupVersionResource) map[string]string {
	return nil
}

type resourceAttributeManager struct {
	logger        *zap.Logger
	dynamicClient dynamic.Interface
	enrichments   []resolvedResourceAttributeEnrichment
	metrics       metrics.Recorder

	mu      sync.RWMutex
	values  map[string]string
	stopChs []chan struct{}
	wg      sync.WaitGroup
}

type resolvedResourceAttributeEnrichment struct {
	config ResourceAttributeEnrichment
	gvr    schema.GroupVersionResource
}

func resolveResourceAttributeEnrichments(
	disc discovery.DiscoveryInterface,
	enrichments []ResourceAttributeEnrichment,
	logger *zap.Logger,
) ([]resolvedResourceAttributeEnrichment, error) {
	if len(enrichments) == 0 {
		return nil, nil
	}
	watches := make([]ObjectWatch, 0, len(enrichments))
	for _, enrichment := range enrichments {
		source := enrichment.ValueFrom.K8sContainerEnv.Object
		watches = append(watches, ObjectWatch{
			Name:       source.Resource,
			Group:      source.Group,
			Version:    source.Version,
			Namespaces: []string{source.Namespace},
		})
	}
	resolved, err := resolveObjectGVRs(disc, watches, logger)
	if err != nil {
		return nil, err
	}
	if len(resolved) != len(enrichments) {
		return nil, fmt.Errorf(
			"internal: resolveObjectGVRs returned %d results for %d enrichments",
			len(resolved), len(enrichments),
		)
	}
	result := make([]resolvedResourceAttributeEnrichment, 0, len(enrichments))
	for i, enrichment := range enrichments {
		result = append(result, resolvedResourceAttributeEnrichment{config: enrichment, gvr: resolved[i].GVR})
	}
	return result, nil
}

func newResourceAttributeManager(
	logger *zap.Logger,
	dynamicClient dynamic.Interface,
	enrichments []resolvedResourceAttributeEnrichment,
	metricsRecorder metrics.Recorder,
) *resourceAttributeManager {
	if metricsRecorder == nil {
		metricsRecorder = metrics.NoopRecorder{}
	}
	return &resourceAttributeManager{
		logger:        logger,
		dynamicClient: dynamicClient,
		enrichments:   enrichments,
		metrics:       metricsRecorder,
		values:        make(map[string]string, len(enrichments)),
	}
}

func (m *resourceAttributeManager) Start(ctx context.Context) error {
	for i := range m.enrichments {
		enrichment := &m.enrichments[i]
		if err := m.startInformer(ctx, enrichment); err != nil {
			return err
		}
	}
	return nil
}

func (m *resourceAttributeManager) Shutdown(context.Context) error {
	m.mu.Lock()
	stopChs := m.stopChs
	m.stopChs = nil
	m.mu.Unlock()

	for _, stopCh := range stopChs {
		close(stopCh)
	}
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.values = make(map[string]string, len(m.enrichments))
	return nil
}

func (m *resourceAttributeManager) AttributesFor(gvr schema.GroupVersionResource) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var attrs map[string]string
	for _, enrichment := range m.enrichments {
		if !enrichment.config.ApplyTo.matches(gvr) {
			continue
		}
		value, exists := m.values[enrichment.config.Key]
		if !exists || value == "" {
			continue
		}
		if attrs == nil {
			attrs = make(map[string]string)
		}
		attrs[enrichment.config.Key] = value
	}
	return attrs
}

func (m *resourceAttributeManager) startInformer(
	ctx context.Context,
	enrichment *resolvedResourceAttributeEnrichment,
) error {
	source := enrichment.config.ValueFrom.K8sContainerEnv
	listOpts := metav1.ListOptions{FieldSelector: fmt.Sprintf("metadata.name=%s", source.Object.Name)}

	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = listOpts.FieldSelector
			return resourceClientFor(m.dynamicClient, enrichment.gvr, source.Object.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = listOpts.FieldSelector
			return resourceClientFor(m.dynamicClient, enrichment.gvr, source.Object.Namespace).Watch(ctx, options)
		},
	}

	informer := cache.NewSharedInformer(lw, &unstructured.Unstructured{}, 0)
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { m.updateValue(enrichment, obj) },
		UpdateFunc: func(_, newObj interface{}) { m.updateValue(enrichment, newObj) },
		DeleteFunc: func(interface{}) {
			m.metrics.RecordEnrichmentValueChange(
				context.Background(), enrichment.config.Key, metrics.EnrichmentValueCleared,
			)
			m.deleteValue(enrichment.config.Key)
		},
	}); err != nil {
		return fmt.Errorf("failed to add event handler for resource attribute enrichment %q: %w", enrichment.config.Key, err)
	}
	stopCh := make(chan struct{})

	m.mu.Lock()
	m.stopChs = append(m.stopChs, stopCh)
	m.wg.Add(1) // must be inside the lock to prevent a Shutdown racing between append and Add
	m.mu.Unlock()

	go func() {
		defer m.wg.Done()
		informer.Run(stopCh)
	}()

	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()
	// Unblock WaitForCacheSync immediately if the informer is stopped before the
	// cache syncs (e.g. Shutdown called during Start).
	go func() {
		select {
		case <-stopCh:
			syncCancel()
		case <-syncCtx.Done():
		}
	}()
	syncOutcome := metrics.EnrichmentSyncSynced
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		syncOutcome = metrics.EnrichmentSyncTimedOut
		m.logger.Warn("Resource attribute enrichment source did not sync",
			zap.String("key", enrichment.config.Key),
			zap.String("gvr", emit.FormatGVRKey(enrichment.gvr)),
		)
	}
	m.metrics.RecordEnrichmentSync(ctx, enrichment.config.Key, syncOutcome)
	return nil
}

func (m *resourceAttributeManager) updateValue(enrichment *resolvedResourceAttributeEnrichment, obj interface{}) {
	u, ok := toUnstructured(obj)
	if !ok {
		return
	}
	value, found, unsupported := extractStaticContainerEnv(
		u,
		enrichment.config.ValueFrom.K8sContainerEnv.Container,
		enrichment.config.ValueFrom.K8sContainerEnv.Env,
	)
	if unsupported {
		m.logger.Warn("Resource attribute enrichment env var uses unsupported valueFrom",
			zap.String("key", enrichment.config.Key),
			zap.String("container", enrichment.config.ValueFrom.K8sContainerEnv.Container),
			zap.String("env", enrichment.config.ValueFrom.K8sContainerEnv.Env),
		)
		m.metrics.RecordEnrichmentValueChange(
			context.Background(), enrichment.config.Key, metrics.EnrichmentValueUnsupported,
		)
		m.deleteValue(enrichment.config.Key)
		return
	}
	if !found {
		m.logger.Debug("Resource attribute enrichment env var is missing",
			zap.String("key", enrichment.config.Key),
			zap.String("container", enrichment.config.ValueFrom.K8sContainerEnv.Container),
			zap.String("env", enrichment.config.ValueFrom.K8sContainerEnv.Env),
		)
		m.metrics.RecordEnrichmentValueChange(
			context.Background(), enrichment.config.Key, metrics.EnrichmentValueCleared,
		)
		m.deleteValue(enrichment.config.Key)
		return
	}

	m.mu.Lock()
	previous, existed := m.values[enrichment.config.Key]
	m.values[enrichment.config.Key] = value
	m.mu.Unlock()
	if !existed || previous != value {
		m.logger.Info("Resource attribute enrichment value available", zap.String("key", enrichment.config.Key))
		m.metrics.RecordEnrichmentValueChange(
			context.Background(), enrichment.config.Key, metrics.EnrichmentValueSet,
		)
	}
}

func (m *resourceAttributeManager) deleteValue(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
}

const (
	specKey           = "spec"
	templateKey       = "template"
	specContainersKey = "containers"
)

// extractStaticContainerEnv searches spec.template.spec.containers then
// spec.template.spec.initContainers for a container named containerName, and
// returns the static value of the env var named envName within that container.
// Returns (value, found, unsupported): unsupported=true when the env entry
// uses valueFrom (dynamic reference) rather than a static literal value.
// Once the named container is found in a slice, the search does not continue
// into subsequent slices even if the env var is absent there.
func extractStaticContainerEnv(obj *unstructured.Unstructured, containerName, envName string) (string, bool, bool) {
	for _, path := range [][]string{
		{specKey, templateKey, specKey, specContainersKey},
		{specKey, templateKey, specKey, "initContainers"},
	} {
		containers, found, _ := unstructured.NestedSlice(obj.Object, path...)
		if !found {
			continue
		}
		for _, rawContainer := range containers {
			container, ok := rawContainer.(map[string]interface{})
			if !ok {
				continue
			}
			name, _, _ := unstructured.NestedString(container, "name")
			if name != containerName {
				continue
			}
			// Container matched — search for the env var here; do not fall through to initContainers.
			envs, found, _ := unstructured.NestedSlice(container, "env")
			if !found {
				return "", false, false
			}
			for _, rawEnv := range envs {
				env, ok := rawEnv.(map[string]interface{})
				if !ok {
					continue
				}
				name, _, _ := unstructured.NestedString(env, "name")
				if name != envName {
					continue
				}
				if _, found, _ := unstructured.NestedMap(env, "valueFrom"); found {
					return "", false, true
				}
				value, found, _ := unstructured.NestedString(env, "value")
				return value, found, false
			}
			return "", false, false
		}
	}
	return "", false, false
}

func (applyTo ResourceAttributeApplyTo) matches(gvr schema.GroupVersionResource) bool {
	return matchesAny(applyTo.APIGroups, gvr.Group) && matchesAny(applyTo.Resources, gvr.Resource)
}

func matchesAny(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern == "*" || pattern == value {
			return true
		}
	}
	return false
}

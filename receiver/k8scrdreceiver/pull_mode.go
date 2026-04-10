package k8scrdreceiver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// pullMode implements the mode interface for periodic CRD/CR collection.
type pullMode struct {
	settings         receiver.Settings
	config           *Config
	consumer         consumer.Logs
	client           k8sClient
	forbiddenTracker *forbiddenTracker

	ticker *time.Ticker
	wg     sync.WaitGroup
	stopCh chan struct{}

	//nolint:containedctx
	ctx    context.Context
	cancel context.CancelFunc
}

func newPullMode(
	settings receiver.Settings,
	config *Config,
	cons consumer.Logs,
	client k8sClient,
	ft *forbiddenTracker,
) *pullMode {
	return &pullMode{
		settings:         settings,
		config:           config,
		consumer:         cons,
		client:           client,
		forbiddenTracker: ft,
	}
}

func (m *pullMode) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.stopCh = make(chan struct{})

	m.settings.Logger.Info("Starting pull mode",
		zap.Duration("interval", m.config.Pull.Interval),
	)

	m.ticker = time.NewTicker(m.config.Pull.Interval)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		for {
			select {
			case <-m.ctx.Done():
				m.settings.Logger.Info("Pull mode stopped - context cancelled")
				return
			case <-m.stopCh:
				m.settings.Logger.Info("Pull mode stopped - stop channel closed")
				return
			case <-m.ticker.C:
				if err := m.performPull(); err != nil {
					m.settings.Logger.Error("Pull failed", zap.Error(err))
				}
			}
		}
	}()

	return nil
}

func (m *pullMode) Shutdown(_ context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.stopCh != nil {
		close(m.stopCh)
	}

	if m.ticker != nil {
		m.ticker.Stop()
	}

	m.wg.Wait()
	return nil
}

func (m *pullMode) performPull() error {
	m.settings.Logger.Debug("Performing pull")

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	crdList, err := m.client.Resource(crdGVR).List(m.ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list CRDs: %w", err)
	}

	for i := range crdList.Items {
		crd := &crdList.Items[i]

		// Check API group filters before emitting CRD or listing CRs
		apiGroup, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		if !m.config.shouldWatchAPIGroup(apiGroup) {
			continue
		}

		if err := emitLog(m.ctx, m.consumer, crd, watch.Added, buildCRDLogRecord); err != nil {
			m.settings.Logger.Error("Failed to emit CRD log",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
		}

		if err := m.pullAndEmitCRsForCRD(crd); err != nil {
			m.settings.Logger.Error("Failed to pull CRs for CRD",
				zap.String("name", crd.GetName()),
				zap.Error(err),
			)
		}
	}

	m.settings.Logger.Debug("Pull completed", zap.Int("crds", len(crdList.Items)))
	return nil
}

func (m *pullMode) pullAndEmitCRsForCRD(crdObj *unstructured.Unstructured) error {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := convertUnstructuredToCRD(crdObj, &crd); err != nil {
		return fmt.Errorf("failed to convert to CRD: %w", err)
	}

	storageVersion := getStorageVersion(&crd)
	if storageVersion == "" {
		return fmt.Errorf("no storage version found for CRD %s", crd.Name)
	}

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  storageVersion,
		Resource: crd.Spec.Names.Plural,
	}

	if shouldRetry, retryIn := m.forbiddenTracker.shouldRetry(gvr); !shouldRetry {
		m.settings.Logger.Debug("Skipping forbidden resource (will retry later)",
			zap.String("gvr", formatGVRKey(gvr)),
			zap.Duration("retry_in", retryIn),
		)
		return nil
	}

	crList, err := m.client.Resource(gvr).List(m.ctx, metav1.ListOptions{})
	if err != nil {
		if isPermissionDenied(err) {
			m.forbiddenTracker.markForbidden(gvr)
			m.settings.Logger.Info("Skipping CR pull - insufficient RBAC permissions",
				zap.String("gvr", formatGVRKey(gvr)),
			)
			return nil
		}
		return fmt.Errorf("failed to list CRs for %s: %w", formatGVRKey(gvr), err)
	}

	for i := range crList.Items {
		cr := &crList.Items[i]
		if err := emitLog(m.ctx, m.consumer, cr, watch.Added, buildCRLogRecord); err != nil {
			m.settings.Logger.Error("Failed to emit CR log",
				zap.String("name", cr.GetName()),
				zap.Error(err),
			)
		}
	}

	return nil
}

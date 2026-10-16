package stsksmreceiver

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	ddlog "github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/log"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ksm"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/pmetricsender"
)

const (
	serviceNamespaceAttribute = "service.namespace"
	clusterNameAttribute      = "k8s.cluster.name"
	transport                 = "kubernetes"
)

type ksmReceiver struct {
	set         receiver.Settings
	cfg         *Config
	next        consumer.Metrics
	obsrecv     *receiverhelper.ObsReport
	clusterName string

	newClient        func() (kubernetes.Interface, error)
	clusterIDTimeout time.Duration
	client           kubernetes.Interface
	clusterID        string

	mu           sync.Mutex
	shuttingDown bool
	cancel       context.CancelFunc
	done         chan struct{}
}

func newReceiver(set receiver.Settings, cfg *Config, next consumer.Metrics) (*ksmReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              transport,
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}
	ddlog.SetLogger(set.Logger)
	return &ksmReceiver{
		set:              set,
		cfg:              cfg,
		next:             next,
		obsrecv:          obsrecv,
		clusterName:      cfg.clusterName(),
		newClient:        func() (kubernetes.Interface, error) { return newKubeClient(cfg.AuthType) },
		clusterIDTimeout: clusterIDTimeout,
	}, nil
}

func (r *ksmReceiver) Start(ctx context.Context, host component.Host) error {
	var elector k8sleaderelector.LeaderElection
	if r.cfg.K8sLeaderElector != nil {
		ext, ok := host.GetExtensions()[*r.cfg.K8sLeaderElector]
		if !ok {
			return fmt.Errorf("k8s leader elector extension %q not found", r.cfg.K8sLeaderElector)
		}
		if elector, ok = ext.(k8sleaderelector.LeaderElection); !ok {
			return fmt.Errorf("extension %q does not implement k8sleaderelector.LeaderElection", r.cfg.K8sLeaderElector)
		}
	}

	client, err := r.newClient()
	if err != nil {
		return fmt.Errorf("failed to create the Kubernetes client: %w", err)
	}
	r.client = client
	if r.clusterID, err = lookupClusterID(ctx, client, r.clusterIDTimeout); err != nil {
		r.set.Logger.Warn("Cannot read the cluster ID; metrics are sent without the orch_cluster_id tag", zap.Error(err))
	}

	if elector == nil {
		r.startCollection()
		return nil
	}
	elector.SetCallBackFuncs(
		func(context.Context) {
			r.set.Logger.Info("Started leading; collecting Kubernetes state metrics")
			r.startCollection()
		},
		func() {
			r.set.Logger.Info("Stopped leading; no longer collecting Kubernetes state metrics")
			r.stopCollection()
		},
	)
	return nil
}

func (r *ksmReceiver) Shutdown(context.Context) error {
	r.mu.Lock()
	r.shuttingDown = true
	r.mu.Unlock()
	r.stopCollection()
	return nil
}

func (r *ksmReceiver) startCollection() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shuttingDown || r.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go r.run(ctx, r.done)
}

func (r *ksmReceiver) stopCollection() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel, r.done = nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// run owns one check and its informers; cancelling ctx stops both.
func (r *ksmReceiver) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	check := r.newCheck()
	var stores [][]cache.Store
	buildStores := func() {
		var err error
		if stores, err = check.BuildStores(ctx, r.client); err != nil {
			r.set.Logger.Error("Cannot start the kube-state-metrics stores; retrying at the next collection", zap.Error(err))
		}
	}
	buildStores()

	ticker := time.NewTicker(r.cfg.CollectionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if stores == nil {
				buildStores()
				continue
			}
			r.collect(ctx, check, stores)
		}
	}
}

func (r *ksmReceiver) collect(ctx context.Context, check *ksm.KSMCheck, stores [][]cache.Store) {
	now := time.Now()
	s := pmetricsender.New(pcommon.NewTimestampFromTime(now))
	check.Collect(s, stores, now)
	md := s.Metrics()
	points := md.DataPointCount()
	if points == 0 {
		return
	}
	r.setResource(md)

	ctx = r.obsrecv.StartMetricsOp(ctx)
	err := r.next.ConsumeMetrics(ctx, md)
	r.obsrecv.EndMetricsOp(ctx, typeStr, points, err)
	if err != nil {
		r.set.Logger.Error("Failed to send Kubernetes state metrics", zap.Error(err))
	}
}

func (r *ksmReceiver) setResource(md pmetric.Metrics) {
	attrs := md.ResourceMetrics().At(0).Resource().Attributes()
	// Empty blocks the platform collector's service.namespace=default insert; VictoriaMetrics drops empty labels.
	attrs.PutStr(serviceNamespaceAttribute, "")
	// Must equal kube_cluster_name: vmagent builds the RBAC _scope_ from k8s_cluster_name when present.
	attrs.PutStr(clusterNameAttribute, ksm.RFC1123ClusterName(r.clusterName))
}

func (r *ksmReceiver) newCheck() *ksm.KSMCheck {
	return ksm.New(r.instanceConfig(), ksm.Options{
		ClusterName:           r.clusterName,
		RFC1123ClusterNameTag: true,
		ClusterID:             r.clusterID,
	})
}

// instanceConfig returns a fresh check configuration: the check mutates it.
func (r *ksmReceiver) instanceConfig() *ksm.KSMConfig {
	return &ksm.KSMConfig{
		Collectors:        slices.Clone(r.cfg.Collectors),
		LabelsAsTags:      cloneMapping(r.cfg.LabelsAsTags),
		AnnotationsAsTags: cloneMapping(r.cfg.AnnotationsAsTags),
		ResyncPeriod:      int(r.cfg.ResyncPeriod / time.Second),
	}
}

func cloneMapping(in map[string]map[string]string) map[string]map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]map[string]string, len(in))
	for kind, mapping := range in {
		out[kind] = maps.Clone(mapping)
	}
	return out
}

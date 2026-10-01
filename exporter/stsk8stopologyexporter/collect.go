package stsk8stopologyexporter

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/topology"
	"github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/apiserver"
	collectors "github.com/stackvista/sts-opentelemetry-collector/exporter/stsk8stopologyexporter/internal/topologycollectors"
	"go.uber.org/zap"
)

var (
	errCollectTimeout = errors.New("kubernetes topology collection did not finish in time")
	errCollectPanic   = errors.New("kubernetes topology collector panicked")
)

type collectResult struct {
	components []*topology.Component
	relations  []*topology.Relation
	errors     []error
}

// collectTopology runs the cluster-agent kubernetes_api_topology collectors
// and correlators over client, following kubeapi.TopologyCheck.Run.
func collectTopology(
	client apiserver.APICollectorClient, instance topology.Instance, clusterType collectors.ClusterType,
	cfg *Config, logger *zap.Logger,
) (collectResult, error) {
	nodeIdentifierCorrelationChannel := make(chan *collectors.NodeIdentifierCorrelation)
	containerCorrelationChannel := make(chan *collectors.ContainerCorrelation)
	volumeCorrelationChannel := make(chan *collectors.VolumeCorrelation)
	podCorrelationChannel := make(chan *collectors.PodLabelCorrelation)
	endpointCorrelationChannel := make(chan *collectors.ServiceSelectorCorrelation)

	componentChannel := make(chan *topology.Component)
	relationChannel := make(chan *topology.Relation)
	errChannel := make(chan error)

	k8sVersion, _ := client.GetVersion()
	common := collectors.NewClusterTopologyCommon(
		instance, clusterType, client, componentChannel, relationChannel, k8sVersion,
	)
	commonCollector := collectors.NewClusterTopologyCollector(common)
	resources := cfg.Resources

	clusterCollectors := []collectors.ClusterTopologyCollector{
		collectors.NewClusterCollector(commonCollector),
		collectors.NewNodeCollector(nodeIdentifierCorrelationChannel, commonCollector),
		collectors.NewPodCollector(
			containerCorrelationChannel, volumeCorrelationChannel, podCorrelationChannel, commonCollector,
		),
		collectors.NewServiceCollector(endpointCorrelationChannel, commonCollector),
	}
	if resources.PersistentVolumes {
		clusterCollectors = append(clusterCollectors,
			collectors.NewPersistentVolumeCollector(commonCollector, cfg.CSIPVMapperEnabled))
	}
	if resources.Namespaces {
		clusterCollectors = append(clusterCollectors, collectors.NewNamespaceCollector(commonCollector))
	}
	if resources.ConfigMaps {
		clusterCollectors = append(clusterCollectors,
			collectors.NewConfigMapCollector(commonCollector, cfg.ConfigMapMaxDataSize))
	}
	if resources.Secrets {
		clusterCollectors = append(clusterCollectors, collectors.NewSecretCollector(commonCollector))
	}
	if resources.DaemonSets {
		clusterCollectors = append(clusterCollectors, collectors.NewDaemonSetCollector(commonCollector))
	}
	if resources.Deployments {
		clusterCollectors = append(clusterCollectors, collectors.NewDeploymentCollector(commonCollector))
	}
	if resources.ReplicaSets {
		clusterCollectors = append(clusterCollectors, collectors.NewReplicaSetCollector(commonCollector))
	}
	if resources.StatefulSets {
		clusterCollectors = append(clusterCollectors, collectors.NewStatefulSetCollector(commonCollector))
	}
	if resources.Ingresses {
		clusterCollectors = append(clusterCollectors, collectors.NewIngressCollector(commonCollector))
	}
	if resources.Jobs {
		clusterCollectors = append(clusterCollectors, collectors.NewJobCollector(commonCollector))
	}
	if resources.CronJobs {
		clusterCollectors = append(clusterCollectors, collectors.NewCronJobCollector(commonCollector))
	}

	commonCorrelator := collectors.NewClusterTopologyCorrelator(common)
	clusterCorrelators := []collectors.ClusterTopologyCorrelator{
		collectors.NewContainerCorrelator(
			nodeIdentifierCorrelationChannel, containerCorrelationChannel, commonCorrelator,
		),
		collectors.NewVolumeCorrelator(volumeCorrelationChannel, commonCorrelator, resources.PersistentVolumeClaims),
		collectors.NewService2PodCorrelator(podCorrelationChannel, endpointCorrelationChannel, commonCorrelator),
	}

	panics := make(chan error, 1+len(clusterCorrelators))
	recoverPanic := func() {
		if r := recover(); r != nil {
			panics <- fmt.Errorf("%w: %v\n%s", errCollectPanic, r, debug.Stack())
		}
	}

	var producers sync.WaitGroup
	producers.Add(1 + len(clusterCorrelators))
	go func() {
		defer producers.Done()
		defer recoverPanic()
		for _, collector := range clusterCollectors {
			if err := collector.CollectorFunction(); err != nil {
				errChannel <- err
			}
		}
	}()
	for _, correlator := range clusterCorrelators {
		go func(correlator collectors.ClusterTopologyCorrelator) {
			defer producers.Done()
			defer recoverPanic()
			if err := correlator.CorrelateFunction(); err != nil {
				errChannel <- err
			}
		}(correlator)
	}
	done := make(chan struct{})
	go func() {
		producers.Wait()
		common.CorrelateRelations()
		close(done)
	}()

	var result collectResult
	timeout := time.NewTimer(cfg.CollectTimeout)
	defer timeout.Stop()
	for {
		select {
		case component := <-componentChannel:
			result.components = append(result.components, component)
		case relation := <-relationChannel:
			result.relations = append(result.relations, relation)
		case err := <-errChannel:
			logger.Warn("Kubernetes topology collector error", zap.Error(err))
			result.errors = append(result.errors, err)
		case err := <-panics:
			go drain(componentChannel, relationChannel, errChannel, done)
			return collectResult{}, err
		case <-done:
			select {
			case err := <-panics:
				return collectResult{}, err
			default:
			}
			return result, nil
		case <-timeout.C:
			// Producers block on the unbuffered channels; drain them so their goroutines exit.
			go drain(componentChannel, relationChannel, errChannel, done)
			return collectResult{}, errCollectTimeout
		}
	}
}

func drain(
	components <-chan *topology.Component, relations <-chan *topology.Relation, errs <-chan error, done <-chan struct{},
) {
	for {
		select {
		case <-components:
		case <-relations:
		case <-errs:
		case <-done:
			return
		}
	}
}

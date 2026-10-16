/*
Copyright 2017 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package customresources

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/kube-state-metrics/v2/pkg/customresource"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var descEndpointLabelsDefaultLabels = []string{"namespace", "endpoint"}

// NewExtendedEndpointFactory returns the Endpoints address count families that
// kube-state-metrics removed after v2.13.
func NewExtendedEndpointFactory(client kubernetes.Interface) customresource.RegistryFactory {
	return &extendedEndpointFactory{
		client: client,
	}
}

type extendedEndpointFactory struct {
	client kubernetes.Interface
}

// Name is the name of the factory
func (f *extendedEndpointFactory) Name() string {
	return "endpoints_extended"
}

func (f *extendedEndpointFactory) CreateClient(_ *rest.Config) (interface{}, error) {
	return f.client, nil
}

// MetricFamilyGenerators returns the extended endpoint metric family generators
func (f *extendedEndpointFactory) MetricFamilyGenerators() []generator.FamilyGenerator {
	return []generator.FamilyGenerator{
		*generator.NewFamilyGeneratorWithStability(
			"kube_endpoint_address_available",
			"Number of addresses available in endpoint.",
			metric.Gauge,
			basemetrics.ALPHA,
			"v2.6.0",
			wrapEndpointFunc(func(e *v1.Endpoints) *metric.Family {
				var available int
				for _, s := range e.Subsets {
					available += len(s.Addresses) * len(s.Ports)
				}

				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							Value: float64(available),
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_endpoint_address_not_ready",
			"Number of addresses not ready in endpoint",
			metric.Gauge,
			basemetrics.ALPHA,
			"v2.6.0",
			wrapEndpointFunc(func(e *v1.Endpoints) *metric.Family {
				var notReady int
				for _, s := range e.Subsets {
					notReady += len(s.NotReadyAddresses) * len(s.Ports)
				}
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							Value: float64(notReady),
						},
					},
				}
			}),
		),
	}
}

func wrapEndpointFunc(f func(*v1.Endpoints) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		endpoint := obj.(*v1.Endpoints)

		metricFamily := f(endpoint)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descEndpointLabelsDefaultLabels, []string{endpoint.Namespace, endpoint.Name}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

// ExpectedType returns the type expected by the factory
func (f *extendedEndpointFactory) ExpectedType() interface{} {
	return &v1.Endpoints{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Endpoints",
			APIVersion: v1.SchemeGroupVersion.String(),
		},
	}
}

// ListWatch returns a ListerWatcher for v1.Endpoints
func (f *extendedEndpointFactory) ListWatch(customResourceClient interface{}, ns string, fieldSelector string) cache.ListerWatcher {
	client := customResourceClient.(kubernetes.Interface)
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = fieldSelector
			return client.CoreV1().Endpoints(ns).List(context.TODO(), opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.FieldSelector = fieldSelector
			return client.CoreV1().Endpoints(ns).Watch(context.TODO(), opts)
		},
	}
}

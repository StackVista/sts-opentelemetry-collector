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

package ksm213

import (
	"strconv"

	v1 "k8s.io/api/core/v1"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var (
	descPodLabelsDefaultLabels = []string{"namespace", "pod", "uid"}
	podStatusReasons           = []string{"Evicted", "NodeAffinity", "NodeLost", "Shutdown", "UnexpectedAdmissionError"}
)

func createPodStatusReasonFamilyGenerator() generator.FamilyGenerator {
	return *generator.NewFamilyGeneratorWithStability(
		"kube_pod_status_reason",
		"The pod status reasons",
		metric.Gauge,
		basemetrics.ALPHA,
		"",
		wrapPodFunc(func(p *v1.Pod) *metric.Family {
			ms := []*metric.Metric{}

			for _, reason := range podStatusReasons {
				metric := &metric.Metric{}
				metric.LabelKeys = []string{"reason"}
				metric.LabelValues = []string{reason}
				if p.Status.Reason == reason {
					metric.Value = boolFloat64(true)
				} else {
					metric.Value = boolFloat64(false)
				}
				ms = append(ms, metric)
			}

			return &metric.Family{
				Metrics: ms,
			}
		}),
	)
}

func createPodTolerationsFamilyGenerator() generator.FamilyGenerator {
	return *generator.NewFamilyGeneratorWithStability(
		"kube_pod_tolerations",
		"Information about the pod tolerations",
		metric.Gauge,
		basemetrics.ALPHA,
		"",
		wrapPodFunc(func(p *v1.Pod) *metric.Family {
			var ms []*metric.Metric

			for _, t := range p.Spec.Tolerations {
				var labelKeys []string
				var labelValues []string

				if t.Key != "" {
					labelKeys = append(labelKeys, "key")
					labelValues = append(labelValues, t.Key)
				}

				if t.Operator != "" {
					labelKeys = append(labelKeys, "operator")
					labelValues = append(labelValues, string(t.Operator))
				}

				if t.Value != "" {
					labelKeys = append(labelKeys, "value")
					labelValues = append(labelValues, t.Value)
				}

				if t.Effect != "" {
					labelKeys = append(labelKeys, "effect")
					labelValues = append(labelValues, string(t.Effect))
				}

				if t.TolerationSeconds != nil {
					labelKeys = append(labelKeys, "toleration_seconds")
					labelValues = append(labelValues, strconv.FormatInt(*t.TolerationSeconds, 10))
				}

				if len(labelKeys) == 0 {
					continue
				}

				ms = append(ms, &metric.Metric{
					LabelKeys:   labelKeys,
					LabelValues: labelValues,
					Value:       1,
				})
			}

			return &metric.Family{
				Metrics: ms,
			}
		}),
	)
}

func wrapPodFunc(f func(*v1.Pod) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		pod := obj.(*v1.Pod)

		metricFamily := f(pod)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descPodLabelsDefaultLabels, []string{pod.Namespace, pod.Name, string(pod.UID)}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

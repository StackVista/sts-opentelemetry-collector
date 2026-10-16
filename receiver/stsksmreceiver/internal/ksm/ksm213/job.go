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
	v1batch "k8s.io/api/batch/v1"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var (
	descJobLabelsDefaultLabels = []string{"namespace", "job_name"}
	jobFailureReasons          = []string{"BackoffLimitExceeded", "DeadlineExceeded", "Evicted"}
)

func createJobStatusFailedFamilyGenerator() generator.FamilyGenerator {
	return *generator.NewFamilyGeneratorWithStability(
		"kube_job_status_failed",
		"The number of pods which reached Phase Failed and the reason for failure.",
		metric.Gauge,
		basemetrics.STABLE,
		"",
		wrapJobFunc(func(j *v1batch.Job) *metric.Family {
			var ms []*metric.Metric

			if float64(j.Status.Failed) == 0 {
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							Value: float64(j.Status.Failed),
						},
					},
				}
			}

			for _, c := range j.Status.Conditions {
				condition := c
				if condition.Type == v1batch.JobFailed {
					reasonKnown := false
					for _, reason := range jobFailureReasons {
						reasonKnown = reasonKnown || failureReason(&condition, reason)

						// for known reasons
						ms = append(ms, &metric.Metric{
							LabelKeys:   []string{"reason"},
							LabelValues: []string{reason},
							Value:       boolFloat64(failureReason(&condition, reason)),
						})
					}
					// for unknown reasons
					if !reasonKnown {
						ms = append(ms, &metric.Metric{
							LabelKeys:   []string{"reason"},
							LabelValues: []string{""},
							Value:       float64(j.Status.Failed),
						})
					}
				}
			}

			return &metric.Family{
				Metrics: ms,
			}
		}),
	)
}

func failureReason(jc *v1batch.JobCondition, reason string) bool {
	if jc == nil {
		return false
	}
	return jc.Reason == reason
}

func wrapJobFunc(f func(*v1batch.Job) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		job := obj.(*v1batch.Job)

		metricFamily := f(job)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descJobLabelsDefaultLabels, []string{job.Namespace, job.Name}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

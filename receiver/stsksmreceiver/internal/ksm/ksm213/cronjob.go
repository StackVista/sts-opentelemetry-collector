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
	batchv1 "k8s.io/api/batch/v1"
	basemetrics "k8s.io/component-base/metrics"
	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var (
	descCronJobLabelsDefaultLabels = []string{"namespace", "cronjob"}
)

func createCronJobSpecSuspendFamilyGenerator() generator.FamilyGenerator {
	return *generator.NewFamilyGeneratorWithStability(
		"kube_cronjob_spec_suspend",
		"Suspend flag tells the controller to suspend subsequent executions.",
		metric.Gauge,
		basemetrics.STABLE,
		"",
		wrapCronJobFunc(func(j *batchv1.CronJob) *metric.Family {
			ms := []*metric.Metric{}

			if j.Spec.Suspend != nil {
				ms = append(ms, &metric.Metric{
					LabelKeys:   []string{},
					LabelValues: []string{},
					Value:       boolFloat64(*j.Spec.Suspend),
				})
			}

			return &metric.Family{
				Metrics: ms,
			}
		}),
	)
}

func wrapCronJobFunc(f func(*batchv1.CronJob) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		cronJob := obj.(*batchv1.CronJob)

		metricFamily := f(cronJob)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descCronJobLabelsDefaultLabels, []string{cronJob.Namespace, cronJob.Name}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

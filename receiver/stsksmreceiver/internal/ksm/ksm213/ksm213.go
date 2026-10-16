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

// Package ksm213 holds metric family generators from the kube-state-metrics
// version the cluster agent pins, for families whose output later versions
// changed in a way the agent's mapping exposes.
package ksm213

import (
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

// Replace swaps each family generator that has a v2.13 counterpart.
func Replace(families []generator.FamilyGenerator) []generator.FamilyGenerator {
	out := make([]generator.FamilyGenerator, len(families))
	for i, f := range families {
		if replacement, ok := generators[f.Name]; ok {
			out[i] = replacement()
		} else {
			out[i] = f
		}
	}
	return out
}

var generators = map[string]func() generator.FamilyGenerator{ //nolint:gochecknoglobals
	"kube_pod_status_reason":    createPodStatusReasonFamilyGenerator,
	"kube_pod_tolerations":      createPodTolerationsFamilyGenerator,
	"kube_job_status_failed":    createJobStatusFailedFamilyGenerator,
	"kube_cronjob_spec_suspend": createCronJobSpecSuspendFamilyGenerator,
}

func boolFloat64(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// mergeKeyValues merges label keys and values slice pairs into a single slice pair.
// Arguments are passed as equal-length pairs of slices, where the first slice contains keys and second contains values.
// Example: mergeKeyValues(keys1, values1, keys2, values2) => (keys1+keys2, values1+values2)
func mergeKeyValues(keyValues ...[]string) (keys, values []string) {
	capacity := 0
	for i := 0; i < len(keyValues); i += 2 {
		capacity += len(keyValues[i])
	}

	// Allocate one contiguous block, then split it up to keys and values zero'd slices.
	keysValues := make([]string, 0, capacity*2)
	keys = (keysValues[0:capacity:capacity])[:0]
	values = (keysValues[capacity : capacity*2])[:0]

	for i := 0; i < len(keyValues); i += 2 {
		keys = append(keys, keyValues[i]...)
		values = append(values, keyValues[i+1]...)
	}

	return keys, values
}

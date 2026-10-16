package ksm

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/mocksender"
	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/sender"
)

// gaugeTags returns the tags of every Gauge call for name that carries all of match.
func gaugeTags(s *mocksender.MockSender, name string, match ...string) [][]string {
	var out [][]string
	for _, c := range s.Calls {
		if c.Method != "Gauge" || c.Arguments.String(0) != name {
			continue
		}
		tags, _ := c.Arguments.Get(3).([]string)
		if !slices.ContainsFunc(match, func(m string) bool { return !slices.Contains(tags, m) }) {
			out = append(out, tags)
		}
	}
	return out
}

func tagWithPrefix(tags []string, prefix string) bool {
	return slices.ContainsFunc(tags, func(t string) bool { return strings.HasPrefix(t, prefix) })
}

func collectParity(t *testing.T, ready []string, objects ...runtime.Object) *mocksender.MockSender {
	t.Helper()
	check := New(&KSMConfig{Collectors: chartCollectors}, Options{ClusterName: "prod"})
	stores := startFakeKSM(t, check, objects...)
	var s *mocksender.MockSender
	newSender := func() sender.Sender {
		s = mocksender.NewMockSender("ksm")
		s.SetupAcceptAll()
		return s
	}
	collectWhen(t, check, stores, newSender, func() bool {
		for _, name := range ready {
			if len(gaugeTags(s, name)) == 0 {
				return false
			}
		}
		return true
	})
	return s
}

func scheduledPod(name string, status corev1.PodStatus) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shop", UID: types.UID("uid-" + name)}, Status: status}
}

func TestPodStatusReasonParity(t *testing.T) {
	s := collectParity(t, []string{"kubernetes_state.pod.scheduled"},
		scheduledPod("gated", corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonSchedulingGated},
		}}),
		scheduledPod("evicted", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		}}),
		scheduledPod("outofcpu", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "OutOfcpu", Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		}}),
	)

	for pod, want := range map[string]string{"gated": "", "evicted": "reason:Evicted", "outofcpu": ""} {
		series := gaugeTags(s, "kubernetes_state.pod.scheduled", "pod_name:"+pod)
		require.NotEmpty(t, series, pod)
		for _, tags := range series {
			if want == "" {
				assert.False(t, tagWithPrefix(tags, "reason:"), "%s: %v", pod, tags)
			} else {
				assert.Contains(t, tags, want, pod)
			}
		}
	}
}

func TestPodTolerationsParity(t *testing.T) {
	seconds := int64(300)
	pod := scheduledPod("tolerant", corev1.PodStatus{})
	pod.Spec.Tolerations = []corev1.Toleration{
		{Operator: corev1.TolerationOpExists},
		{},
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	s := collectParity(t, []string{"kubernetes_state.pod.tolerations"}, pod)

	series := gaugeTags(s, "kubernetes_state.pod.tolerations", "pod_name:tolerant")
	require.Len(t, series, 2, "the agent emits no series for an empty toleration")
	for _, tags := range series {
		for _, empty := range []string{"key:", "value:", "effect:", "toleration_seconds:"} {
			assert.NotContains(t, tags, empty, "the agent omits empty toleration fields")
		}
	}
	assert.Len(t, gaugeTags(s, "kubernetes_state.pod.tolerations", "operator:Exists", "key:node.kubernetes.io/not-ready",
		"effect:NoExecute", "toleration_seconds:300"), 1)
}

func TestJobStatusFailedParity(t *testing.T) {
	s := collectParity(t, []string{"kubernetes_state.job.failed"},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "retrying", Namespace: "shop", UID: "job-1"}, Status: batchv1.JobStatus{Failed: 2}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "clean", Namespace: "shop", UID: "job-2"}},
	)

	assert.Empty(t, gaugeTags(s, "kubernetes_state.job.failed", "kube_job:retrying"),
		"the agent emits no failure series before the Job has a Failed condition")
	s.AssertMetric(t, "Gauge", "kubernetes_state.job.failed", 0, "", []string{"kube_job:clean"})
}

func TestCronJobSpecSuspendParity(t *testing.T) {
	suspended := false
	s := collectParity(t, []string{"kubernetes_state.cronjob.spec_suspend"},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "unset", Namespace: "shop", UID: "cj-1"}, Spec: batchv1.CronJobSpec{Schedule: "@hourly"}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "shop", UID: "cj-2"},
			Spec: batchv1.CronJobSpec{Schedule: "@hourly", Suspend: &suspended}},
	)

	assert.Empty(t, gaugeTags(s, "kubernetes_state.cronjob.spec_suspend", "kube_cronjob:unset"))
	s.AssertMetric(t, "Gauge", "kubernetes_state.cronjob.spec_suspend", 0, "", []string{"kube_cronjob:running"})
}

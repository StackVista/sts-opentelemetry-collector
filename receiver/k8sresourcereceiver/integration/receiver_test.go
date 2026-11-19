//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
	"unicode/utf8"

	k8sreceiver "github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func testReceiver(ctx context.Context, t *testing.T, client kubernetes.Interface, admin dynamic.Interface,
	extensions extclient.Interface, config *rest.Config,
) {
	for _, ns := range []string{testNS, teamA, teamB} {
		namespace(ctx, t, client, ns)
	}
	account := serviceAccount(ctx, t, client, config, "runtime")
	actor := dynamicClient(t, account)
	role(ctx, t, client, "runtime-control",
		readRule("observability.suse.com", "collectiondeclarations"),
		rbacv1.PolicyRule{APIGroups: []string{"observability.suse.com"},
			Resources: []string{"collectiondeclarations/status"}, Verbs: []string{"patch", "update"}},
		readRule("apiextensions.k8s.io", "customresourcedefinitions"), readRule("", "pods"))
	binding(ctx, t, client, "runtime-control", "runtime-control", "runtime", "")
	a := labeled(selection("*", teamA), "app", "alpha")
	b := labeled(selection("widgets", teamA, teamB), "monitoring", "enabled")
	createDeclaration(ctx, t, admin, declaration("a", a))
	saveKubeconfig(t, account)
	sink := &consumertest.LogsSink{}
	factory := k8sreceiver.NewFactory()
	cfg, ok := factory.CreateDefaultConfig().(*k8sreceiver.Config)
	require.True(t, ok)
	cfg.APIConfig.AuthType = k8sreceiver.AuthTypeKubeConfig
	cfg.ClusterName = "rc-runtime"
	cfg.CustomResourceAPIGroups = &k8sreceiver.APIGroups{Include: []string{}}
	cfg.Objects = []k8sreceiver.ObjectWatch{{Name: "pods"}}
	cfg.DeniedObjects = []k8sreceiver.ObjectMatcher{{Name: "gadgets", Group: testGroup}}
	cfg.MaxDeclarationWatches = 4
	cfg.IncrementInterval = time.Second
	cfg.SnapshotInterval = time.Minute
	cfg.EmitSnapshotBoundaries = true
	cfg.CompactCache = os.Getenv("RC_COMPACT") == "true"
	require.NoError(t, cfg.Validate())
	core, logs := observer.New(zap.DebugLevel)
	settings := componenttest.NewNopTelemetrySettings()
	settings.Logger = zap.New(core)
	runtime, err := factory.CreateLogs(ctx, receiver.Settings{
		ID: component.NewID(factory.Type()), TelemetrySettings: settings,
	}, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, runtime.Start(ctx, componenttest.NewNopHost()))
	t.Cleanup(func() {
		stopctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		require.NoError(t, runtime.Shutdown(stopctx))
		writeEvidence(t, "records.json", []byte(jsonString(t, records(sink))))
		encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		var encoded []byte
		for _, entry := range logs.All() {
			buffer, err := encoder.EncodeEntry(entry.Entry, entry.Context)
			require.NoError(t, err)
			encoded = append(encoded, buffer.Bytes()...)
			buffer.Free()
		}
		writeEvidence(t, "receiver.jsonl", encoded)
	})

	waitStatus(ctx, t, admin, "a", "Pending")
	require.Eventually(t, func() bool {
		all := records(sink)
		for _, record := range all {
			obj := object(record)
			if obj.GetKind() == "Pod" && len(completeSnapshots(all)) > 0 {
				return true
			}
		}
		return false
	}, 90*time.Second, 200*time.Millisecond, "missing custom API must not prevent native snapshots")
	installCRD(ctx, t, extensions, customCRD("widgets", "Widget", extv1.NamespaceScoped))
	for _, row := range []struct {
		ns, name string
		labels   map[string]string
	}{
		{teamA, "both", map[string]string{"app": "alpha", "monitoring": "enabled"}},
		{teamA, "only-a", map[string]string{"app": "alpha"}},
		{teamA, "cross-a", map[string]string{"app": "beta"}},
		{teamB, "only-b", map[string]string{"app": "beta", "monitoring": "enabled"}},
		{teamB, "cross-b", map[string]string{"app": "alpha"}},
		{teamB, "neither", map[string]string{"app": "gamma"}},
		{teamB, "baseline", map[string]string{"app": "gamma", "baseline": "enabled"}},
	} {
		_, err := resources(admin, "widgets", row.ns).Create(ctx,
			selectedObject("Widget", row.ns, row.name, row.labels), metav1.CreateOptions{})
		require.NoError(t, err)
	}
	waitStatus(ctx, t, admin, "a", "Blocked")
	require.Empty(t, objectState(records(sink)))
	role(ctx, t, client, "runtime-widgets", readRule(testGroup, "widgets"))
	binding(ctx, t, client, "runtime-widgets", "runtime-widgets", "runtime", teamA)
	require.Eventually(t, func() bool {
		_, err := resources(actor, "widgets", teamA).List(ctx, metav1.ListOptions{})
		return err == nil
	}, 15*time.Second, 200*time.Millisecond)
	// Editing the selection creates a fresh watch; unchanged-watch backoff has fake-clock coverage.
	a["fieldSelector"] = "metadata.name!=backoff-probe-unmatched"
	replaceSelection(ctx, t, admin, declaration("a", a))
	waitStatus(ctx, t, admin, "a", "Active")
	onlyA := []string{"widgets/" + teamA + "/both", "widgets/" + teamA + "/only-a"}
	waitObjects(t, sink, onlyA)
	createDeclaration(ctx, t, admin, declaration("b", b))
	waitStatus(ctx, t, admin, "b", "Blocked")
	waitObjects(t, sink, onlyA)
	binding(ctx, t, client, "runtime-widgets", "runtime-widgets", "runtime", teamB)
	require.Eventually(t, func() bool {
		_, err := resources(actor, "widgets", teamB).List(ctx, metav1.ListOptions{})
		return err == nil
	}, 15*time.Second, 200*time.Millisecond)
	b["fieldSelector"] = "metadata.name!=backoff-probe-unmatched"
	replaceSelection(ctx, t, admin, declaration("b", b))
	waitStatus(ctx, t, admin, "b", "Active")
	union := []string{"widgets/" + teamA + "/both", "widgets/" + teamA + "/only-a", "widgets/" + teamB + "/only-b"}
	waitObjects(t, sink, union)
	_, err = resources(actor, "widgets", "").List(ctx, metav1.ListOptions{})
	require.True(t, apierrors.IsForbidden(err), "namespaced grants must not allow cluster-wide reads: %v", err)
	require.NoError(t, declarations(admin).Delete(ctx, "rc-test-a", metav1.DeleteOptions{}))
	onlyB := []string{"widgets/" + teamA + "/both", "widgets/" + teamB + "/only-b"}
	waitObjects(t, sink, onlyB)
	for _, value := range []string{`null`, `"enabled"`} {
		_, err = resources(admin, "widgets", teamB).Patch(ctx, "only-b", types.MergePatchType,
			[]byte(`{"metadata":{"labels":{"monitoring":`+value+`}}}`), metav1.PatchOptions{})
		require.NoError(t, err)
		expected := onlyB
		if value == `null` {
			expected = []string{"widgets/" + teamA + "/both"}
		}
		waitObjects(t, sink, expected)
	}
	installCRD(ctx, t, extensions, customCRD("gadgets", "Gadget", extv1.NamespaceScoped))
	_, err = resources(admin, "gadgets", teamA).Create(ctx,
		selectedObject("Gadget", teamA, "denied", map[string]string{"app": "alpha"}), metav1.CreateOptions{})
	require.NoError(t, err)
	createDeclaration(ctx, t, admin, declaration("a", labeled(selection("*", teamA), "app", "alpha")))
	waitStatus(ctx, t, admin, "a", "Blocked")
	waitObjects(t, sink, union)
	createDeclaration(ctx, t, admin, declaration("native", map[string]any{"group": "apps", "resource": "deployments"}))
	waitStatus(ctx, t, admin, "native", "Blocked")
	installCRD(ctx, t, extensions, customCRD("fleets", "Fleet", extv1.ClusterScoped))
	createDeclaration(ctx, t, admin, declaration("scope", selection("fleets", teamA)))
	waitStatus(ctx, t, admin, "scope", "Blocked")
	field := selection("widgets", teamA)
	field["fieldSelector"] = "metadata.name=both"
	createDeclaration(ctx, t, admin, declaration("field", field))
	waitStatus(ctx, t, admin, "field", "Active")
	field["fieldSelector"] = "spec.value=initial"
	replaceSelection(ctx, t, admin, declaration("field", field))
	waitStatus(ctx, t, admin, "field", "Blocked")
	budget := selection("widgets", teamA)
	budget["fieldSelector"] = "metadata.name=cross-a"
	createDeclaration(ctx, t, admin, declaration("z-budget", budget))
	blocked := waitStatus(ctx, t, admin, "z-budget", "Blocked")
	results, found, err := unstructured.NestedSlice(blocked.Object, "status", "resources")
	require.NoError(t, err)
	require.True(t, found)
	budgetResult, ok := results[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "WatchBudgetExceeded", budgetResult["reason"])
	waitObjects(t, sink, union)
	createDeclaration(ctx, t, admin, declaration("z-shared", b))
	waitStatus(ctx, t, admin, "z-shared", "Active")
	createDeclaration(ctx, t, admin, declaration("z-self",
		map[string]any{"group": "observability.suse.com", "resource": "collectiondeclarations"}))
	waitStatus(ctx, t, admin, "z-self", "Active")
	require.NoError(t, declarations(admin).Delete(ctx, "rc-test-field", metav1.DeleteOptions{}))
	waitStatus(ctx, t, admin, "z-budget", "Active")
	waitObjects(t, sink, append(append([]string(nil), union...), "widgets/"+teamA+"/cross-a"))
	require.NoError(t, declarations(admin).Delete(ctx, "rc-test-z-budget", metav1.DeleteOptions{}))
	require.NoError(t, declarations(admin).Delete(ctx, "rc-test-z-shared", metav1.DeleteOptions{}))
	require.NoError(t, declarations(admin).Delete(ctx, "rc-test-z-self", metav1.DeleteOptions{}))
	waitObjects(t, sink, union)
	createDeclaration(ctx, t, admin, declaration("maximum", repeatedSelections(32, absentSelection)...))
	states := make([]string, 32)
	for i := range states {
		states[i] = "Pending"
	}
	maximum := waitStatus(ctx, t, admin, "maximum", states...)
	conditions, found, err := unstructured.NestedSlice(maximum.Object, "status", "conditions")
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, conditions)
	ready, ok := conditions[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "False", ready["status"])
	entries, found, err := unstructured.NestedSlice(maximum.Object, "status", "resources")
	require.NoError(t, err)
	require.True(t, found)
	for i, raw := range entries {
		entry, ok := raw.(map[string]any)
		require.True(t, ok)
		require.Equal(t, int64(i), entry["selectionIndex"])
		if message, ok := entry["message"].(string); ok {
			require.LessOrEqual(t, utf8.RuneCountInString(message), 2048)
		}
	}
	require.Eventually(t, func() bool {
		for _, record := range records(sink) {
			obj := object(record)
			results, _, _ := unstructured.NestedSlice(obj.Object, "status", "resources")
			if obj.GetKind() == "CollectionDeclaration" && len(results) > 0 {
				return true
			}
		}
		return false
	}, time.Minute, 200*time.Millisecond, "declaration status must be exported")
	waitObjects(t, sink, union)
	require.Eventually(t, func() bool {
		snapshots := completeSnapshots(records(sink))
		if len(snapshots) == 0 {
			return false
		}
		last := snapshots[len(snapshots)-1]
		return sameObjects(objectState(last), union) && uniqueObjects(last)
	}, 90*time.Second, 200*time.Millisecond, "complete snapshot must contain the union once")
	writeEvidence(t, "maximum-status.json", []byte(jsonString(t, maximum.Object)))
}

func labeled(entry map[string]any, key, value string) map[string]any {
	entry["labelSelector"] = map[string]any{"matchLabels": map[string]any{key: value}}
	return entry
}

func waitStatus(ctx context.Context, t *testing.T, client dynamic.Interface, name string, states ...string) *unstructured.Unstructured {
	t.Helper()
	var current *unstructured.Unstructured
	require.Eventually(t, func() bool {
		obj, err := declarations(client).Get(ctx, "rc-test-"+name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		current = obj
		generation, _, _ := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
		entries, _, _ := unstructured.NestedSlice(obj.Object, "status", "resources")
		if generation != obj.GetGeneration() || len(entries) != len(states) {
			return false
		}
		for i, raw := range entries {
			entry, ok := raw.(map[string]any)
			if !ok || entry["state"] != states[i] {
				return false
			}
		}
		return true
	}, time.Minute, 200*time.Millisecond, "declaration %s did not reach %v", name, states)
	writeEvidence(t, "status-"+name+".json", []byte(jsonString(t, current.Object)))
	return current
}

func jsonString(t *testing.T, value any) string {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return string(data)
}

//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authzv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func testAdmission(ctx context.Context, t *testing.T, client dynamic.Interface) {
	group := func(value string) map[string]any {
		entry := selection("widgets")
		entry["group"] = value
		return entry
	}
	labels := func(values map[string]any) map[string]any {
		entry := selection("widgets")
		entry["labelSelector"] = map[string]any{"matchLabels": values}
		return entry
	}
	expression := func(operator string, values ...string) map[string]any {
		entry := map[string]any{"key": "app", "operator": operator}
		if values != nil {
			entry["values"] = values
		}
		return map[string]any{"group": testGroup, "resource": "widgets",
			"labelSelector": map[string]any{"matchExpressions": []any{entry}}}
	}
	field := func(value string) map[string]any {
		entry := selection("widgets")
		entry["fieldSelector"] = value
		return entry
	}
	values := make([]string, 33)
	for i := range values {
		values[i] = fmt.Sprint(i)
	}
	tooManyLabels := make(map[string]any)
	for i := range 17 {
		tooManyLabels[fmt.Sprintf("key-%d", i)] = "v"
	}
	namespaces := make([]string, 129)
	for i := range namespaces {
		namespaces[i] = fmt.Sprintf("ns-%d", i)
	}
	cases := []struct {
		name       string
		valid      bool
		selections []map[string]any
	}{
		{"missing-api", true, []map[string]any{selection("widgets")}},
		{"native-group-runtime-check", true, []map[string]any{{"group": "apps", "resource": "deployments"}}},
		{"wildcard", true, []map[string]any{selection("*")}},
		{"empty-label-value", true, []map[string]any{labels(map[string]any{"valid.example/Key": ""})}},
		{"maximum-selections", true, repeatedSelections(32, func(int) map[string]any { return selection("widgets") })},
		{"maximum-selector", true, []map[string]any{maximumSelection()}},
		{"maximum-combined", true, repeatedSelections(32, func(int) map[string]any { return maximumSelection() })},
		{"empty", false, nil},
		{"core", false, []map[string]any{{"group": "", "resource": "pods"}}},
		{"core-wildcard", false, []map[string]any{{"group": "", "resource": "*"}}},
		{"group-wildcard", false, []map[string]any{group("*")}},
		{"group-uppercase", false, []map[string]any{group("Invalid.example")}},
		{"resource-name", false, []map[string]any{selection("Widgets")}},
		{"empty-resource", false, []map[string]any{selection("")}},
		{"empty-namespaces", false, []map[string]any{selection("widgets", []string{}...)}},
		{"namespace-name", false, []map[string]any{selection("widgets", "Invalid")}},
		{"namespace-overflow", false, []map[string]any{selection("widgets", namespaces...)}},
		{"selection-overflow", false, repeatedSelections(33, func(int) map[string]any { return selection("widgets") })},
		{"invalid-label-key", false, []map[string]any{labels(map[string]any{"invalid/key/again": "value"})}},
		{"invalid-label-value", false, []map[string]any{labels(map[string]any{"valid": "bad value"})}},
		{"label-overflow", false, []map[string]any{labels(tooManyLabels)}},
		{"in-empty", false, []map[string]any{expression("In", []string{}...)}},
		{"notin-missing", false, []map[string]any{expression("NotIn")}},
		{"exists-values", false, []map[string]any{expression("Exists", "alpha")}},
		{"invalid-operator", false, []map[string]any{expression("Equals")}},
		{"values-overflow", false, []map[string]any{expression("In", values...)}},
		{"missing-group", false, []map[string]any{{"resource": "widgets"}}},
		{"valid-in", true, []map[string]any{expression("In", "alpha")}},
		{"valid-notin", true, []map[string]any{expression("NotIn", "alpha")}},
		{"valid-exists", true, []map[string]any{expression("Exists")}},
		{"valid-doesnotexist", true, []map[string]any{expression("DoesNotExist")}},
		{"field-name", true, []map[string]any{field("metadata.name=active")}},
		{"field-escaped", true, []map[string]any{field(`metadata.name!=comma\,equals\=slash\\`)}},
		{"field-invalid", false, []map[string]any{field("metadata.name!==active")}},
		{"field-no-operator", false, []map[string]any{field("metadata.name")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := declaration(tc.name, tc.selections...)
			stored, err := declarations(client).Create(ctx, obj, metav1.CreateOptions{FieldValidation: "Strict"})
			if !tc.valid {
				require.True(t, apierrors.IsInvalid(err), "expected admission rejection, got %v", err)
				return
			}
			require.NoError(t, err)
			// JSON normalizes fixture integers and slices to the API's unstructured representation.
			require.JSONEq(t, jsonString(t, obj.Object["spec"]), jsonString(t, stored.Object["spec"]))
			require.NoError(t, declarations(client).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}))
		})
	}
	obj := declaration("update", selection("widgets"))
	createDeclaration(ctx, t, client, obj)
	before, err := declarations(client).Get(ctx, obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	_, err = declarations(client).Patch(ctx, obj.GetName(), types.MergePatchType,
		[]byte(`{"spec":{"resources":[]}}`), metav1.PatchOptions{})
	require.True(t, apierrors.IsInvalid(err), "expected invalid update, got %v", err)
	after, err := declarations(client).Get(ctx, obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, before.GetGeneration(), after.GetGeneration())
	require.Equal(t, before.Object["spec"], after.Object["spec"])
	require.NoError(t, declarations(client).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}))
	pinned := selection("widgets")
	pinned["version"] = "v1"
	_, err = declarations(client).Create(ctx, declaration("version", pinned), metav1.CreateOptions{FieldValidation: "Strict"})
	require.ErrorContains(t, err, "unknown field")
}

func maximumSelection() map[string]any {
	prefix := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	namespaces := make([]string, 128)
	for i := range namespaces {
		namespaces[i] = fmt.Sprintf("ns-%03d", i)
	}
	labels := make(map[string]any)
	expressions := make([]any, 16)
	values := make([]string, 32)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}
	for i := range expressions {
		labels[fmt.Sprintf("%s/label-%d", prefix, i)] = strings.Repeat("v", 63)
		expressions[i] = map[string]any{"key": prefix + "/" + strings.Repeat("e", 63), "operator": "In", "values": values}
	}
	entry := selection("widgets", namespaces...)
	entry["labelSelector"] = map[string]any{"matchLabels": labels, "matchExpressions": expressions}
	return entry
}

func testAuthorization(ctx context.Context, t *testing.T, client kubernetes.Interface, admin dynamic.Interface, config *rest.Config) {
	namespace(ctx, t, client, testNS)
	write := rbacv1.PolicyRule{APIGroups: []string{"observability.suse.com"}, Resources: []string{"resourcecollections"},
		Verbs: []string{"get", "list", "watch", "create", "delete", "patch", "update"}}
	statusRule := rbacv1.PolicyRule{APIGroups: []string{"observability.suse.com"},
		Resources: []string{"resourcecollections/status"}, Verbs: []string{"patch", "update"}}
	role(ctx, t, client, "integration", write)
	role(ctx, t, client, "collector", readRule("observability.suse.com", "resourcecollections"), statusRule)
	obj := declaration("auth", selection("widgets"))
	createDeclaration(ctx, t, admin, obj)
	defer func() { require.NoError(t, declarations(admin).Delete(ctx, obj.GetName(), metav1.DeleteOptions{})) }()
	for _, name := range []string{"integration", "namespace-only", "collector"} {
		t.Run(name, func(t *testing.T) {
			account := serviceAccount(ctx, t, client, config, name)
			roleName, ns := name, ""
			if name == "namespace-only" {
				roleName, ns = "integration", testNS
			}
			binding(ctx, t, client, name, roleName, name, ns)
			actor := dynamicClient(t, account)
			actorKube, err := kubernetes.NewForConfig(account)
			require.NoError(t, err)
			for _, verb := range write.Verbs {
				allowed := name == "integration" || name == "collector" && (verb == "get" || verb == "list" || verb == "watch")
				review, err := actorKube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authzv1.SelfSubjectAccessReview{
					Spec: authzv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authzv1.ResourceAttributes{
						Group: "observability.suse.com", Resource: "resourcecollections", Verb: verb,
					}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
				require.Equal(t, allowed, review.Status.Allowed, "%s %s", name, verb)
			}
			if name == "collector" {
				_, err = declarations(actor).Get(ctx, obj.GetName(), metav1.GetOptions{})
				require.NoError(t, err)
				_, err = declarations(actor).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.NoError(t, patchStatus(ctx, actor, obj.GetName(), map[string]any{"observedGeneration": int64(1)}))
				_, err = declarations(actor).Patch(ctx, obj.GetName(), types.MergePatchType,
					[]byte(`{"spec":{"resources":[]}}`), metav1.PatchOptions{})
				require.True(t, apierrors.IsForbidden(err), "expected spec PATCH forbidden, got %v", err)
				testStatusAdmission(ctx, t, actor, admin)
				require.NoError(t, client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}))
				require.Eventually(t, func() bool {
					return apierrors.IsForbidden(patchStatus(ctx, actor, obj.GetName(), map[string]any{"observedGeneration": int64(1)}))
				}, 15*time.Second, 200*time.Millisecond, "revoked status permission still works")
				return
			}
			created, err := declarations(actor).Create(ctx, declaration(name, selection("widgets")), metav1.CreateOptions{})
			if name == "namespace-only" {
				require.True(t, apierrors.IsForbidden(err), "expected cluster-scoped CREATE forbidden, got %v", err)
			} else {
				require.NoError(t, err)
				require.NoError(t, declarations(actor).Delete(ctx, created.GetName(), metav1.DeleteOptions{}))
			}
		})
	}
}

func maximumStatus() map[string]any {
	entries := make([]any, 32)
	for i := range entries {
		entries[i] = map[string]any{"selectionIndex": int64(i), "group": strings.Repeat("g", 253),
			"resource": strings.Repeat("r", 63), "state": "Blocked", "reason": strings.Repeat("R", 128),
			"message": strings.Repeat("😀", 2048)}
	}
	return map[string]any{"observedGeneration": int64(1), "resources": entries, "conditions": []any{
		map[string]any{"type": "Ready", "status": "False", "observedGeneration": int64(1),
			"reason": strings.Repeat("R", 128), "message": strings.Repeat("😀", 2048),
			"lastTransitionTime": "2026-11-16T00:00:00Z"},
	}}
}

func testStatusAdmission(ctx context.Context, t *testing.T, actor, admin dynamic.Interface) {
	obj := declaration("status", repeatedSelections(32, func(int) map[string]any { return selection("widgets") })...)
	createDeclaration(ctx, t, admin, obj)
	before, err := declarations(admin).Get(ctx, obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	accepted := maximumStatus()
	require.NoError(t, patchStatus(ctx, actor, obj.GetName(), accepted))
	assertPreserved := func(t *testing.T) {
		t.Helper()
		stored, err := declarations(admin).Get(ctx, obj.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		require.JSONEq(t, jsonString(t, accepted), jsonString(t, stored.Object["status"]))
		require.Equal(t, before.Object["spec"], stored.Object["spec"])
		require.Equal(t, before.GetGeneration(), stored.GetGeneration())
	}
	assertPreserved(t)
	for _, name := range []string{"message", "condition-message", "entries", "index", "duplicate-index"} {
		t.Run("status-"+name, func(t *testing.T) {
			invalid := maximumStatus()
			entries, ok := invalid["resources"].([]any)
			require.True(t, ok)
			first, ok := entries[0].(map[string]any)
			require.True(t, ok)
			switch name {
			case "message":
				first["message"] = strings.Repeat("😀", 2049)
			case "condition-message":
				conditions, ok := invalid["conditions"].([]any)
				require.True(t, ok)
				condition, ok := conditions[0].(map[string]any)
				require.True(t, ok)
				condition["message"] = strings.Repeat("😀", 2049)
			case "entries":
				invalid["resources"] = append(entries, map[string]any{"selectionIndex": int64(32),
					"group": testGroup, "resource": "widgets", "state": "Pending", "reason": "Absent"})
			case "index":
				first["selectionIndex"] = int64(32)
			case "duplicate-index":
				second, ok := entries[1].(map[string]any)
				require.True(t, ok)
				second["selectionIndex"] = int64(0)
			}
			require.True(t, apierrors.IsInvalid(patchStatus(ctx, actor, obj.GetName(), invalid)))
			assertPreserved(t)
		})
	}
	require.NoError(t, declarations(admin).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}))
}

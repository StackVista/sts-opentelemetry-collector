//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/yaml"
)

const (
	testGroup = "collection-test.example.com"
	testNS    = "rc-acceptance"
	teamA     = "rc-team-a"
	teamB     = "rc-team-b"
)

func declarations(client dynamic.Interface) dynamic.ResourceInterface {
	return client.Resource(schema.GroupVersionResource{
		Group: "observability.suse.com", Version: "v1alpha1", Resource: "resourcecollections",
	})
}

func TestResourceCollection(t *testing.T) {
	image := os.Getenv("RC_NODE_IMAGE")
	require.NotEmpty(t, image, "set RC_NODE_IMAGE to a digest-pinned K3s image")
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	node, err := k3s.Run(ctx, image)
	testcontainers.CleanupContainer(t, node)
	require.NoError(t, err)
	kubeconfig, err := node.GetKubeConfig(ctx)
	require.NoError(t, err)
	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	require.NoError(t, err)
	config.Timeout = 15 * time.Second
	client, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		logctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		events, logerr := client.CoreV1().Events("").List(logctx, metav1.ListOptions{})
		if logerr != nil {
			t.Logf("Kubernetes diagnostics: %v", logerr)
			return
		}
		writeEvidence(t, "cluster-events.json", []byte(jsonString(t, events)))
	})
	version, err := client.Discovery().ServerVersion()
	require.NoError(t, err)
	require.Equal(t, os.Getenv("RC_KUBERNETES_MINOR"), version.Major+"."+version.Minor)
	writeEvidence(t, "server-version.json", []byte(jsonString(t, version)))
	dyn, err := dynamic.NewForConfig(config)
	require.NoError(t, err)
	extensions, err := extclient.NewForConfig(config)
	require.NoError(t, err)
	data, err := os.ReadFile("../resourcecollection/resourcecollection-crd.yaml")
	require.NoError(t, err)
	var crd extv1.CustomResourceDefinition
	require.NoError(t, yaml.UnmarshalStrict(data, &crd))
	installCRD(ctx, t, extensions, &crd)

	t.Run("admission", func(t *testing.T) { testAdmission(ctx, t, dyn) })
	t.Run("authorization", func(t *testing.T) { testAuthorization(ctx, t, client, dyn, config) })
	t.Run("receiver", func(t *testing.T) { testReceiver(ctx, t, client, dyn, extensions, config) })
}

func installCRD(ctx context.Context, t *testing.T, client extclient.Interface, crd *extv1.CustomResourceDefinition) {
	t.Helper()
	_, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		for _, condition := range current.Status.Conditions {
			if condition.Type == extv1.Established && condition.Status == extv1.ConditionTrue {
				return true
			}
		}
		return false
	}, time.Minute, 200*time.Millisecond, "CRD %s was not established", crd.Name)
}

func customCRD(resource, kind string, scope extv1.ResourceScope) *extv1.CustomResourceDefinition {
	return &extv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: resource + "." + testGroup},
		Spec: extv1.CustomResourceDefinitionSpec{
			Group: testGroup, Scope: scope,
			Names: extv1.CustomResourceDefinitionNames{Plural: resource, Kind: kind},
			Versions: []extv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &extv1.CustomResourceValidation{OpenAPIV3Schema: &extv1.JSONSchemaProps{
					Type: "object", Properties: map[string]extv1.JSONSchemaProps{
						"spec": {Type: "object", Properties: map[string]extv1.JSONSchemaProps{"value": {Type: "string"}}},
					},
				}},
			}},
		},
	}
}

func selection(resource string, namespaces ...string) map[string]any {
	result := map[string]any{"group": testGroup, "resource": resource}
	if namespaces != nil {
		result["namespaces"] = namespaces
	}
	return result
}

func declaration(name string, selections ...map[string]any) *unstructured.Unstructured {
	entries := make([]any, len(selections))
	for i, entry := range selections {
		entries[i] = entry
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "observability.suse.com/v1alpha1", "kind": "ResourceCollection",
		"metadata": map[string]any{"name": "rc-test-" + name},
		"spec":     map[string]any{"resources": entries},
	}}
}

func createDeclaration(ctx context.Context, t *testing.T, client dynamic.Interface, obj *unstructured.Unstructured) {
	t.Helper()
	_, err := declarations(client).Create(ctx, obj, metav1.CreateOptions{FieldValidation: "Strict"})
	require.NoError(t, err)
}

func replaceSelection(ctx context.Context, t *testing.T, client dynamic.Interface, obj *unstructured.Unstructured) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"spec": obj.Object["spec"]})
	require.NoError(t, err)
	_, err = declarations(client).Patch(ctx, obj.GetName(), types.MergePatchType, data, metav1.PatchOptions{})
	require.NoError(t, err)
}

func patchStatus(ctx context.Context, client dynamic.Interface, name string, status map[string]any) error {
	data, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return err
	}
	_, err = declarations(client).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{}, "status")
	return err
}

func namespace(ctx context.Context, t *testing.T, client kubernetes.Interface, name string) {
	t.Helper()
	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
}

func serviceAccount(ctx context.Context, t *testing.T, client kubernetes.Interface, config *rest.Config, name string) *rest.Config {
	t.Helper()
	_, err := client.CoreV1().ServiceAccounts(testNS).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	token, err := client.CoreV1().ServiceAccounts(testNS).CreateToken(ctx, name, &authv1.TokenRequest{}, metav1.CreateOptions{})
	require.NoError(t, err)
	result := rest.AnonymousClientConfig(config)
	result.BearerToken = token.Status.Token
	return result
}

func role(ctx context.Context, t *testing.T, client kubernetes.Interface, name string, rules ...rbacv1.PolicyRule) {
	t.Helper()
	_, err := client.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules,
	}, metav1.CreateOptions{})
	require.NoError(t, err)
}

func binding(ctx context.Context, t *testing.T, client kubernetes.Interface, name, roleName, account, ns string) {
	t.Helper()
	ref := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName}
	subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: account, Namespace: testNS}}
	var err error
	if ns == "" {
		_, err = client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: ref, Subjects: subjects,
		}, metav1.CreateOptions{})
	} else {
		_, err = client.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: ref, Subjects: subjects,
		}, metav1.CreateOptions{})
	}
	require.NoError(t, err)
}

func saveKubeconfig(t *testing.T, config *rest.Config) {
	t.Helper()
	data, err := clientcmd.Write(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {
			Server: config.Host, CertificateAuthorityData: config.CAData,
		}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {Token: config.BearerToken}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	t.Setenv("KUBECONFIG", path)
}

func writeEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := os.Getenv("RC_ARTIFACTS")
	if dir == "" {
		return
	}
	root, err := os.OpenRoot(dir)
	require.NoError(t, err, "create RC_ARTIFACTS before running the tests")
	defer func() { require.NoError(t, root.Close()) }()
	require.NoError(t, root.WriteFile(name, data, 0o600))
}

func dynamicClient(t *testing.T, config *rest.Config) dynamic.Interface {
	t.Helper()
	client, err := dynamic.NewForConfig(config)
	require.NoError(t, err)
	return client
}

func readRule(group, resource string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: []string{group}, Resources: []string{resource}, Verbs: []string{"get", "list", "watch"}}
}

func selectedObject(kind, namespace, name string, labels map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testGroup + "/v1", "kind": kind, "spec": map[string]any{"value": "initial"},
	}}
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetLabels(labels)
	return obj
}

func resources(client dynamic.Interface, resource, namespace string) dynamic.ResourceInterface {
	return client.Resource(schema.GroupVersionResource{Group: testGroup, Version: "v1", Resource: resource}).Namespace(namespace)
}

func repeatedSelections(count int, build func(int) map[string]any) []map[string]any {
	result := make([]map[string]any, count)
	for i := range result {
		result[i] = build(i)
	}
	return result
}

func absentSelection(index int) map[string]any {
	return selection(fmt.Sprintf("absent-%03d", index))
}

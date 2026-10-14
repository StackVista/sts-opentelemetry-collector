package stsk8seventsexporter

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	orchClusterIDTag = "orch_cluster_id"
	// The cluster agent retries for as long before sending events without the tag.
	clusterIDTimeout       = 10 * time.Second
	clusterIDRetryInterval = time.Second
)

func newKubeClient(authType string) (kubernetes.Interface, error) {
	var restConfig *rest.Config
	var err error
	switch authType {
	case authTypeServiceAccount:
		restConfig, err = rest.InClusterConfig()
	case authTypeKubeConfig:
		restConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	default:
		return nil, fmt.Errorf("invalid auth_type %q", authType)
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(restConfig)
}

// lookupClusterID returns the kube-system namespace UID, which the cluster agent
// uses as the cluster ID.
func lookupClusterID(ctx context.Context, client kubernetes.Interface) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, clusterIDTimeout)
	defer cancel()
	for {
		namespace, err := client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
		if err == nil {
			return string(namespace.UID), nil
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(clusterIDRetryInterval):
		}
	}
}

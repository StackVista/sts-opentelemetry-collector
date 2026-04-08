package k8scrdreceiver

import (
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// AuthType describes the type of authentication to use for kubernetes API
type AuthType string

const (
	// AuthTypeNone means no authentication
	AuthTypeNone AuthType = "none"
	// AuthTypeServiceAccount uses pod service account
	AuthTypeServiceAccount AuthType = "serviceAccount"
	// AuthTypeKubeConfig uses local kubeconfig file
	AuthTypeKubeConfig AuthType = "kubeConfig"
)

// APIConfig contains options relevant to connecting to the K8s API
type APIConfig struct {
	// How to authenticate to the K8s API server.  This can be one of `none`
	// (for no auth), `serviceAccount` (to use the standard service account
	// token provided to the collector pod), or `kubeConfig` to use credentials
	// from `~/.kube/config`.
	AuthType AuthType `mapstructure:"auth_type"`
}

// MakeDynamicClient creates a Kubernetes dynamic client.
//
// A dynamic client is necessary for this receiver because we discover CRDs at runtime
// and need to watch arbitrary custom resources without compile-time knowledge of their types.
// Unlike typed clients (clientset.CoreV1().Pods(), etc.) which require Go structs,
// the dynamic client works with any GroupVersionResource and returns unstructured data
// (map[string]interface{}) that we can serialize and emit as OTLP logs.
func MakeDynamicClient(apiConf APIConfig) (dynamic.Interface, error) {
	authType := apiConf.AuthType
	if authType == "" {
		authType = AuthTypeServiceAccount
	}

	var (
		restConfig *rest.Config
		err        error
	)

	switch authType {
	case AuthTypeKubeConfig:
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
		restConfig, err = kubeConfig.ClientConfig()
	case AuthTypeServiceAccount:
		restConfig, err = rest.InClusterConfig()
	case AuthTypeNone:
		restConfig = &rest.Config{}
	default:
		return nil, fmt.Errorf("invalid authType for kubernetes: %s", authType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes rest config: %w", err)
	}

	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes dynamic client: %w", err)
	}

	return client, nil
}

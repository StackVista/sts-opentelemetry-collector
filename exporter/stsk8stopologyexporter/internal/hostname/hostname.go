// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package hostname

import (
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
)

// clusterName stands in for the agent's global cluster_name configuration.
var clusterName atomic.Value

// SetClusterName sets the cluster name used to build node hostnames.
func SetClusterName(name string) {
	clusterName.Store(name)
}

// GetHostname returns the hostname for a Kubernetes Node. This always
// uses the name known in Kubernetes for the node and the cluster name to build
// the hostname. The Agent Helm chart ensures that the same host name
// is set on the agent pods such that all data sources give the same hostname
// in Kubernetes installations.
func GetHostname(node v1.Node) (string, error) {
	name, _ := clusterName.Load().(string)
	return fmt.Sprintf("%s-%s", node.Name, name), nil
}

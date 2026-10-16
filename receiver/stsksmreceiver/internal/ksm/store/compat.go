package store

// agentKSMCompat undoes label changes made by kube-state-metrics after v2.13,
// the version the cluster agent pins, so the mapped series match the agent.
// It returns false for series that version did not emit.
func agentKSMCompat(family string, labels map[string]string) bool {
	switch family {
	case "kube_ingress_path":
		delete(labels, "path_type")
	case "kube_service_info":
		delete(labels, "external_traffic_policy")
	case "kube_pod_spec_volumes_persistentvolumeclaims_info", "kube_pod_spec_volumes_persistentvolumeclaims_readonly":
		if labels["ephemeral"] == "true" {
			return false
		}
		delete(labels, "ephemeral")
	}
	return true
}

# Initial API

Introduces cluster-scoped `observability.suse.com/v1alpha1` ResourceCollection.
One served/storage version; no existing ResourceCollection instances to migrate.
Compatible additions are the first evolution strategy. Future representation
changes require served-version conversion; storage migration is separate.

The initial selection contract is CRD-backed custom resources only, with exact
nonempty `group`, plural `resource`, optional `namespaces`, structured
`labelSelector`, and `fieldSelector`. Native Kubernetes resources
remain in existing baseline configuration. Collection rejects discovered types
without a backing CRD; absent APIs remain valid pending requests.

Declaration version pinning and native-resource selection were removed before
initial publication. Earlier unpublished candidate manifests containing
`spec.resources[].version` must be updated; strict API field validation should
reject that field, while default handling may prune it. No released instances
require conversion or storage migration. The chart must import this canonical
definition unchanged. Real API admission remains a qualification requirement.

Declarations and baseline group discovery use each CRD's served storage version.
Static YAML keeps explicit pins and preferred-discovery defaults. Full GVR remains
part of watch/cache identity and emitted representation. Different YAML versions
coexist with declaration storage representations without changing other sources.
Fixtures and regressions cover empty-group rejection, field-selector syntax,
standard label operators, mixed storage-version wildcards, and YAML compatibility.

The standalone receiver defaults declaration consumption off. The agent chart
enables consumption explicitly and installs declaration/status RBAC. A missing
ResourceCollection API leaves baseline collection running.

Exact administrator `denied_objects` restrictions now also apply to discovered
custom resources. Group include/exclude remains baseline discovery filtering.
An explicit static selection that contradicts denied_objects retains its existing
configuration validation failure.

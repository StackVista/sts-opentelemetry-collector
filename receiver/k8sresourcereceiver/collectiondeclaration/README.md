# CollectionDeclaration API

This package owns the wire types and CRD consumed by the Kubernetes resource receiver.
V1 declarations select only CRD-backed custom resources. Required `group` and
plural `resource` identify the type. `namespaces`, structured `labelSelector`,
and `fieldSelector` are optional. Empty groups, native Kubernetes resources,
and aggregated APIs without a backing CRD are outside this declaration contract.

Declarations have no `version` field. Each type uses its served storage version,
including types selected through `resource: "*"`. The wildcard expands only
CRD-backed types in the exact group, including types installed later. Missing
APIs or an unusable storage-version API remain pending; healthy types continue.

Existing YAML selections keep explicit version pins and preferred-discovery
defaults. Baseline group discovery and declarations share storage-version
watches when their scopes and filters also match.

`labelSelector` uses the same Kubernetes type as Pod affinity: `matchLabels` and
`matchExpressions`, with `In`, `NotIn`, `Exists`, and `DoesNotExist`. Requirements
are ANDed; separate selections are ORed. Omitted or empty label selectors add no
label filter. Namespace selection currently uses explicit names; omitting
`namespaces` selects all namespaces, subject to administrator restrictions and
permissions. Namespace-label selection is deferred beyond v1.

`fieldSelector` uses the Kubernetes string syntax: `=`, `==`, or `!=`, with
comma-separated clauses combined by AND. Escape commas, equals signs, and
backslashes inside values. Admission rejects malformed syntax; which field names
a resource supports is checked by its API during collection. Labels, fields, and
namespaces inside one selection are ANDed; separate selections are ORed.

```yaml
apiVersion: observability.suse.com/v1alpha1
kind: CollectionDeclaration
metadata:
  name: monitored-widgets
spec:
  resources:
    - group: collection-test.example.com
      resource: widgets
      namespaces: [team-a]
      labelSelector:
        matchLabels:
          monitoring: enabled
      fieldSelector: metadata.name!=paused
```

Watches share only the same group/version/resource, namespace, and canonical
label/field selectors. Version remains part of cache identity and emitted
representation. An existing YAML selection of a different version keeps its
representation alongside the declaration's storage-version representation.

Export the canonical definition from the collector repository root:

```sh
go run ./receiver/k8sresourcereceiver/collectiondeclaration/cmd/export \
  -output /tmp/collectiondeclarations.observability.suse.com.yaml
sha256sum /tmp/collectiondeclarations.observability.suse.com.yaml
git rev-parse HEAD
```

Chart imports retain the source commit and exported SHA-256. Do not independently
edit a chart copy.

This is the first CollectionDeclaration API release. Canonical CRD export
verification, Go regressions and real API admission checks cover the initial
contract. Keep additions backward-compatible. Once a released CRD is available,
add schema compatibility checks against that release and relevant upgrade tests.

Status is collector-local progress for the observed generation. It does not
acknowledge backend delivery or topology creation.

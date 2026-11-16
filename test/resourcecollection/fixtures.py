"""Small Kubernetes fixtures and explicitly enumerated expected object sets."""

import copy

GROUP = "collection-test.example.com"
API = "observability.suse.com/v1alpha1"
RESOURCE = "resourcecollections.observability.suse.com"
NAMESPACE = "rc-acceptance"


def declaration(name, selections):
    return {"apiVersion": API, "kind": "ResourceCollection",
            "metadata": {"name": f"rc-test-{name}"}, "spec": {"resources": selections}}


def select(resource, *, group=GROUP, namespaces=None, labels=None):
    result = {"group": group, "resource": resource}
    if namespaces is not None:
        result["namespaces"] = namespaces
    if labels is not None:
        result["labelSelector"] = {"matchLabels": labels}
    return result


DECLARATIONS = {
    "a": declaration("a", [select("*", namespaces=["rc-team-a"], labels={"app": "alpha"})]),
    "b": declaration("b", [select("widgets", namespaces=["rc-team-a", "rc-team-b"],
                                  labels={"monitoring": "enabled"})]),
    "core": declaration("core", [
        select("pods", group="", namespaces=["rc-team-a"], labels={"app": "alpha"}),
        select("services", group="", namespaces=["rc-team-a"], labels={"app": "alpha"}),
    ]),
    "cross": declaration("cross", [
        select("widgets", namespaces=["rc-team-a"], labels={"app": "alpha"}),
        select("widgets", namespaces=["rc-team-b"], labels={"app": "beta"}),
    ]),
    "empty": declaration("empty", [select("widgets", labels={"app": "absent"})]),
}

# These sets are handwritten, never derived by evaluating the declared selectors.
EXPECTED = {
    "baseline": ["widgets/rc-team-b/baseline"],
    "a": ["widgets/rc-team-b/baseline", "widgets/rc-team-a/both", "widgets/rc-team-a/only-a"],
    "a+b": ["widgets/rc-team-b/baseline", "widgets/rc-team-a/both",
            "widgets/rc-team-a/only-a", "widgets/rc-team-b/only-b"],
    "b": ["widgets/rc-team-b/baseline", "widgets/rc-team-a/both", "widgets/rc-team-b/only-b"],
    "a+b+core": ["widgets/rc-team-b/baseline", "widgets/rc-team-a/both",
                 "widgets/rc-team-a/only-a", "widgets/rc-team-b/only-b",
                 "pods/rc-team-a/selected", "services/rc-team-a/selected"],
    "a+gadgets": ["widgets/rc-team-b/baseline", "widgets/rc-team-a/both",
                  "widgets/rc-team-a/only-a", "gadgets/rc-team-a/new-type"],
    "cross": ["widgets/rc-team-a/both", "widgets/rc-team-a/only-a",
              "widgets/rc-team-b/only-b"],
}


def resource_crd(resource):
    kind = {"widgets": "Widget", "gadgets": "Gadget", "fleets": "Fleet"}[resource]
    return {
        "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
        "metadata": {"name": f"{resource}.{GROUP}"},
        "spec": {"group": GROUP, "scope": "Cluster" if resource == "fleets" else "Namespaced",
                 "names": {"plural": resource, "singular": resource[:-1], "kind": kind},
                 "versions": [{"name": "v1", "served": True, "storage": True,
                               "schema": {"openAPIV3Schema": {
                                   "type": "object", "properties": {
                                       "spec": {"type": "object", "properties": {
                                           "value": {"type": "string"}}}}}}}]}}


def objects():
    rows = [
        ("rc-team-a", "both", {"app": "alpha", "monitoring": "enabled"}),
        ("rc-team-a", "only-a", {"app": "alpha"}),
        ("rc-team-a", "cross-a", {"app": "beta"}),
        ("rc-team-b", "only-b", {"app": "beta", "monitoring": "enabled"}),
        ("rc-team-b", "cross-b", {"app": "alpha"}),
        ("rc-team-b", "neither", {"app": "gamma"}),
        ("rc-team-b", "baseline", {"app": "gamma", "baseline": "enabled"}),
    ]
    return [{"apiVersion": f"{GROUP}/v1", "kind": "Widget",
             "metadata": {"namespace": namespace, "name": name, "labels": labels},
             "spec": {"value": "initial"}} for namespace, name, labels in rows]


def maximum_selection():
    prefix = ".".join(["a" * 63, "b" * 63, "c" * 63, "d" * 61])
    key = prefix + "/" + "e" * 63
    return select("widgets", namespaces=[f"ns-{index:03d}" for index in range(128)],
                  labels={f"label-{index}": "v" * 63 for index in range(16)}) | {
        "labelSelector": {
            "matchLabels": {f"{prefix}/label-{index}": "v" * 63 for index in range(16)},
            "matchExpressions": [
                {"key": key, "operator": "In", "values": [f"v{value}" for value in range(32)]}
                for _ in range(16)]}}


def maximum_status():
    return {
        "observedGeneration": 1,
        "conditions": [{
            "type": "Ready", "status": "False", "observedGeneration": 1,
            "reason": "R" * 128, "message": "😀" * 2048,
            "lastTransitionTime": "2026-11-16T00:00:00Z",
        }],
        "resources": [{
            "selectionIndex": index, "group": "g" * 253, "resource": "r" * 63,
            "state": "Blocked", "reason": "R" * 128, "message": "😀" * 2048,
        } for index in range(32)],
    }


def admission_cases():
    base = select("widgets")
    valid = [
        ("missing-api", [base]),
        ("native-group-runtime-check", [select("deployments", group="apps")]),
        ("wildcard", [select("*")]),
        ("empty-label-value", [select("widgets", labels={"valid.example/Key": ""})]),
        ("maximum-selections", [copy.deepcopy(base) for _ in range(32)]),
        ("maximum-selector", [maximum_selection()]),
        ("maximum-combined", [maximum_selection() for _ in range(32)]),
    ]
    cases = [(name, declaration(name, selections), True) for name, selections in valid]
    invalid = [
        ("empty", []), ("core", [select("pods", group="")]),
        ("core-wildcard", [select("*", group="")]),
        ("group-wildcard", [select("widgets", group="*")]),
        ("group-uppercase", [select("widgets", group="Invalid.example")]),
        ("resource-name", [select("Widgets")]), ("empty-resource", [select("")]),
        ("empty-namespaces", [select("widgets", namespaces=[])]),
        ("namespace-name", [select("widgets", namespaces=["Invalid"])]),
        ("namespace-overflow", [select("widgets", namespaces=[f"ns-{i}" for i in range(129)])]),
        ("selection-overflow", [copy.deepcopy(base) for _ in range(33)]),
        ("invalid-label-key", [select("widgets", labels={"invalid/key/again": "value"})]),
        ("invalid-label-value", [select("widgets", labels={"valid": "bad value"})]),
        ("label-overflow", [select("widgets", labels={f"key-{i}": "v" for i in range(17)})]),
    ]
    cases.extend((name, declaration(name, selections), False) for name, selections in invalid)
    for name, expression in [
        ("in-empty", {"key": "app", "operator": "In", "values": []}),
        ("notin-missing", {"key": "app", "operator": "NotIn"}),
        ("exists-values", {"key": "app", "operator": "Exists", "values": ["alpha"]}),
        ("invalid-operator", {"key": "app", "operator": "Equals"}),
        ("values-overflow", {"key": "app", "operator": "In",
                             "values": [str(i) for i in range(33)]}),
    ]:
        selection = base | {"labelSelector": {"matchExpressions": [expression]}}
        cases.append((name, declaration(name, [selection]), False))
    missing_group = declaration("missing-group", [{"resource": "widgets"}])
    cases.append(("missing-group", missing_group, False))
    for operator, values in [
        ("In", ["alpha"]), ("NotIn", ["alpha"]),
        ("Exists", None), ("DoesNotExist", None),
    ]:
        expression = {"key": "app", "operator": operator}
        if values is not None:
            expression["values"] = values
        selection = base | {"labelSelector": {"matchExpressions": [expression]}}
        cases.append((f"valid-{operator.lower()}", declaration(
            operator.lower(), [selection]), True))
    for name, selector, valid in [
        ("field-name", "metadata.name=active", True),
        ("field-escaped", r"metadata.name!=comma\,equals\=slash\\", True),
        ("field-invalid", "metadata.name!==active", False),
        ("field-no-operator", "metadata.name", False),
    ]:
        cases.append((name, declaration(name, [base | {"fieldSelector": selector}]), valid))
    return cases

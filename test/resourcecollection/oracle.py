"""Capture assertions independent of the receiver's selection planner."""

from collections import Counter

GROUP = "collection-test.example.com"
KINDS = {"Widget": "widgets", "Gadget": "gadgets", "Fleet": "fleets"}
NAMESPACES = {"rc-team-a", "rc-team-b"}


def identity(record):
    obj = (record.get("body") or {}).get("object", {})
    metadata = obj.get("metadata", {})
    api_version = obj.get("apiVersion", "")
    if api_version.split("/")[0] == GROUP and obj.get("kind") in KINDS:
        resource = KINDS[obj["kind"]]
    elif api_version == "v1" and obj.get("kind") in {"Pod", "Service"}:
        resource = {"Pod": "pods", "Service": "services"}[obj["kind"]]
    else:
        return None
    namespace = metadata.get("namespace", "")
    if namespace and namespace not in NAMESPACES:
        return None
    return "/".join((resource, namespace, metadata["name"]))


def assert_objects(records, expected, *, unique=False):
    observed = {}
    counts = Counter()
    for record in records:
        key = identity(record)
        if key is None:
            continue
        if record["body"].get("type") == "DELETED":
            observed.pop(key, None)
        else:
            observed[key] = record["body"]["object"]
            counts[key] += 1
    if unique:
        duplicates = sorted(key for key, count in counts.items() if count > 1)
        if duplicates:
            raise AssertionError(f"duplicate snapshot objects: {duplicates}")
    missing = sorted(set(expected) - observed.keys())
    extra = sorted(observed.keys() - set(expected))
    if missing or extra:
        raise AssertionError(f"missing={missing}; extra={extra}")
    return observed


def complete_snapshots(records):
    """Discard resets, interleaved producers, mismatched IDs and incomplete ends."""
    snapshots, current, snapshot_id = [], None, None
    for record in records:
        attrs = record.get("attributes", {})
        boundary = attrs.get("k8s.snapshot.boundary")
        if boundary == "start":
            current, snapshot_id = [], attrs.get("k8s.snapshot.id")
        elif boundary == "reset":
            current, snapshot_id = None, None
        elif boundary == "end":
            if (current is not None and snapshot_id
                    and attrs.get("k8s.snapshot.id") == snapshot_id
                    and attrs.get("k8s.snapshot.complete") is True):
                snapshots.append(current)
            current, snapshot_id = None, None
        elif current is not None:
            current.append(record)
    return snapshots

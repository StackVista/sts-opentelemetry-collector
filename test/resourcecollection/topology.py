"""Actual legacy intake checks for the chart's topology compatibility profile."""

import base64
import copy
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time

from oracle import complete_snapshots
from run import record_case, wait_for

NAMESPACE = "rc-lifecycle"
COLLECTOR = "rc-lifecycle-k8s-resource-collector"
CONFIGMAP_ID = "urn:kubernetes:/rc-lifecycle:rc-native:configmap/baseline"
SECRET_ID = "urn:kubernetes:/rc-lifecycle:rc-native:secret/baseline"
SECRET_VALUE = "fixture-sensitive-value"


class Intake:
    def __init__(self, artifacts):
        self.payloads, self.snapshots, self.pending, self.errors = [], [], {}, []
        self.lock = threading.Lock()
        self.file = (artifacts / "intake.jsonl").open("w")
        intake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    assert self.path == "/stsAgent/intake", self.path
                    assert self.headers["sts-api-key"] == "fixture-only"
                    assert self.headers["Content-Encoding"] == "gzip"
                    raw = gzip.decompress(self.rfile.read(int(self.headers["Content-Length"])))
                    payload = json.loads(raw)
                    assert SECRET_VALUE not in raw.decode()
                    assert base64.b64encode(SECRET_VALUE.encode()).decode() not in raw.decode()
                    producer = payload["internalHostname"]
                    assert producer
                    with intake.lock:
                        intake.payloads.append(payload)
                        intake.file.write(json.dumps(payload) + "\n")
                        intake.file.flush()
                        for topology in payload["topologies"]:
                            assert topology["instance"] == {
                                "type": "kubernetes", "url": "rc-lifecycle"}
                            assert isinstance(topology["components"], list)
                            assert isinstance(topology["relations"], list)
                            assert isinstance(topology["delete_ids"], list)
                            if topology["start_snapshot"]:
                                assert producer not in intake.pending
                                intake.pending[producer] = {
                                    "producer": producer, "components": [], "relations": []}
                            assert producer in intake.pending, "chunk without start"
                            current = intake.pending[producer]
                            current["components"].extend(topology["components"])
                            current["relations"].extend(topology["relations"])
                            if topology["stop_snapshot"]:
                                components = current["components"]
                                ids = [entry["externalId"] for entry in components]
                                assert len(ids) == len(set(ids)), "duplicate legacy components"
                                current["received_at"] = time.monotonic()
                                intake.snapshots.append(intake.pending.pop(producer))
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b"{}")
                except Exception as error:
                    with intake.lock:
                        intake.errors.append(str(error))
                    self.send_error(400)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def read(self):
        with self.lock:
            assert not self.errors, self.errors
            return list(self.snapshots)

    def native_snapshot(self, producer=None, deleted=False, after=0):
        snapshots = [s for s in self.read()[after:]
                     if producer is None or s["producer"] == producer]
        assert snapshots, "No completed legacy snapshot"
        latest = snapshots[-1]
        components = {entry["externalId"]: entry for entry in latest["components"]}
        assert SECRET_ID in components, "Secret missing from complete native snapshot"
        assert (CONFIGMAP_ID in components) != deleted, "Unexpected ConfigMap presence"
        secret = components[SECRET_ID]
        assert secret["type"]["name"] == "secret"
        assert secret["data"]["identifiers"] == [SECRET_ID]
        assert any(entry["type"]["name"] == "pod" for entry in components.values())
        return latest

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.file.close()


def configure(values, gateway, intake):
    values["stackstate"]["url"] = f"http://{gateway}:{intake.server.server_port}/stsAgent"
    values["global"]["features"] = {"experimentalOtelKubernetesClusterAgent": True}
    values["clusterAgent"] = {
        "enabled": True, "replicaCount": 0,
        "config": {"topology": {"collectionInterval": 5}},
        "collection": {
            "kubernetesTopology": True, "kubernetesEvents": False,
            "kubeStateMetrics": {"enabled": False},
            "kubernetesResources": {
                "namespaces": True, "configmaps": True, "secrets": True,
            },
        },
    }


def native_feedback(client):
    client.apply({"apiVersion": "v1", "kind": "Namespace",
                  "metadata": {"name": "rc-native"}})
    client.apply({"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": "baseline", "namespace": "rc-native"},
                  "data": {"mode": "fixture"}})
    client.apply({"apiVersion": "v1", "kind": "Secret",
                  "metadata": {"name": "baseline", "namespace": "rc-native"},
                  "type": "Opaque",
                  "data": {"credential": base64.b64encode(SECRET_VALUE.encode()).decode()}})


def topology_checks(client, capture, intake, artifacts, report, stable, feedback,
                    declaration, old_leader, peer_snapshot):
    snapshots = complete_snapshots(capture.read())
    assert snapshots, "No complete observer snapshots"
    for snapshot in snapshots:
        identities = []
        for record in snapshot:
            obj = (record.get("body") or {}).get("object", {})
            if obj:
                metadata = obj["metadata"]
                identities.append((obj["apiVersion"], obj["kind"],
                                   metadata.get("namespace", ""), metadata["name"],
                                   metadata["uid"]))
        assert len(identities) == len(set(identities)), "Duplicate observer snapshot objects"
    record_case(report, "complete-observer-snapshots-have-no-duplicate-objects", "passed")
    intake_cursor = len(intake.read())
    client.run("delete", "configmap", "baseline", "-n", "rc-native", "--wait=true")
    wait_for("native deletion in actual legacy intake",
             lambda: intake.native_snapshot(producer=old_leader, deleted=True,
                                            after=intake_cursor), timeout=100)
    record_case(report, "native-deletion-removes-only-the-deleted-component", "passed")
    assert SECRET_VALUE not in json.dumps(capture.read())
    assert base64.b64encode(SECRET_VALUE.encode()).decode() not in json.dumps(capture.read())
    record_case(report, "native-secret-sanitized-in-both-export-paths", "passed")

    role = client.get("clusterrole", COLLECTOR)
    blocked_role = copy.deepcopy(role)
    for rule in blocked_role["rules"]:
        if rule.get("apiGroups") == [""] and "configmaps" in rule.get("resources", []):
            rule["resources"].remove("configmaps")
    blocked_role["rules"] = [r for r in blocked_role["rules"]
                             if r.get("resources") or r.get("nonResourceURLs")]
    assert blocked_role["rules"] != role["rules"]
    client.apply(blocked_role)
    pod_ids = stable()
    expected_leader = (pod_ids.keys() - {old_leader}).pop()
    frames = peer_snapshot(client, expected_leader, artifacts)
    assert any(frame.get("type") == "object" and
               (frame.get("gvr") or {}).get("Resource") == "secrets" and
               frame["obj"]["metadata"].get("namespace") == "rc-native" and
               frame["obj"]["metadata"]["name"] == "baseline" for frame in frames)
    assert client.run("auth", "can-i", "list", "configmaps", "--all-namespaces",
                      f"--as=system:serviceaccount:{NAMESPACE}:{COLLECTOR}",
                      check=False).stdout.strip() == "no"
    capture_cursor = len(capture.read())

    def leader_ready():
        frames = peer_snapshot(client, expected_leader, artifacts)
        assert frames and frames[0]["source"] == "leader"
        assert stable()[expected_leader] == pod_ids[expected_leader]
        return True

    node = client.get("pod", old_leader, NAMESPACE)["spec"]["nodeName"]
    client.run("cordon", node)
    try:
        client.run("delete", "pod", old_leader, "-n", NAMESPACE, "--wait=false")
        def populated_leader():
            frames = peer_snapshot(client, expected_leader, artifacts)
            assert frames and frames[0]["source"] == "leader"
            return True
        wait_for("populated follower acquires the lease", populated_leader, timeout=120)
    finally:
        client.run("uncordon", node)
    wait_for("populated follower becomes leader with a blocked static watch",
             leader_ready, timeout=100)
    capture_cursor = len(capture.read())
    wait_for("incomplete observer boundary", lambda: any(
        record["attributes"].get("k8s.snapshot.boundary") == "end" and
        record["attributes"].get("k8s.snapshot.complete") is False
        for record in capture.read()[capture_cursor:]), timeout=100)
    assert not complete_snapshots(capture.read()[capture_cursor:])
    started = time.monotonic()
    while time.monotonic() - started < 75:
        assert not any(s["producer"] == expected_leader for s in intake.read())
        assert not complete_snapshots(capture.read()[capture_cursor:])
        time.sleep(1)
    record_case(report, "incomplete-native-takeover-suppresses-legacy-intake", "passed")

    role["metadata"]["resourceVersion"] = client.get("clusterrole", COLLECTOR)["metadata"]["resourceVersion"]
    client.apply(role)
    wait_for("current declaration remains active after core recovery",
             lambda: feedback(declaration, "Active"))
    wait_for("complete observer snapshot after static-read recovery",
             lambda: complete_snapshots(capture.read()[capture_cursor:]), timeout=100)
    wait_for("new producer exports native topology after static-read recovery",
             lambda: intake.native_snapshot(producer=expected_leader, deleted=True),
             timeout=150)
    assert stable()[expected_leader] == pod_ids[expected_leader]
    record_case(report, "native-read-recovery-resumes-intake-without-pod-restart", "passed")
    (artifacts / "legacy-snapshots.json").write_text(json.dumps(intake.read(), indent=2))

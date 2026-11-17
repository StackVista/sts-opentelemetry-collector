"""Real API and export checks using the built agent and an owned cluster."""

import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading

from fixtures import DECLARATIONS, GROUP, NAMESPACE, RESOURCE
from fixtures import declaration, objects, resource_crd, select
from oracle import assert_objects, complete_snapshots
from run import command, free_port, record_case, wait_for


def value(raw):
    if "kvlistValue" in raw:
        return {entry["key"]: value(entry["value"])
                for entry in raw["kvlistValue"].get("values", [])}
    if "arrayValue" in raw:
        return [value(entry) for entry in raw["arrayValue"].get("values", [])]
    for key in ("stringValue", "boolValue", "doubleValue", "bytesValue"):
        if key in raw:
            return raw[key]
    if "intValue" in raw:
        return int(raw["intValue"])
    return None


class Capture:
    def __init__(self, artifacts):
        self.records, self.errors = [], []
        self.lock = threading.Lock()
        self.file = (artifacts / "records.jsonl").open("w")
        capture = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    raw = self.rfile.read(int(self.headers["Content-Length"]))
                    if self.headers.get("Content-Encoding") == "gzip":
                        raw = gzip.decompress(raw)
                    request = json.loads(raw)
                    for resource in request.get("resourceLogs", []):
                        for scope in resource.get("scopeLogs", []):
                            for record in scope.get("logRecords", []):
                                normalized = {
                                    "body": value(record.get("body", {})),
                                    "attributes": {
                                        entry["key"]: value(entry["value"])
                                        for entry in record.get("attributes", [])},
                                    "eventName": record.get("eventName"),
                                }
                                with capture.lock:
                                    capture.records.append(normalized)
                                    capture.file.write(json.dumps(normalized) + "\n")
                                    capture.file.flush()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b"{}")
                except Exception as error:
                    with capture.lock:
                        capture.errors.append(str(error))
                    self.send_error(400)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def read(self):
        with self.lock:
            if self.errors:
                raise AssertionError(f"Capture decoding failed: {self.errors}")
            return list(self.records)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.file.close()


def qualify(client, args, artifacts, report):
    binary = Path(args.agent_binary).resolve()
    if not binary.is_file():
        raise RuntimeError("Provide the built candidate --agent-binary")
    for namespace in (NAMESPACE, "rc-team-a", "rc-team-b"):
        client.apply({"apiVersion": "v1", "kind": "Namespace",
                      "metadata": {"name": namespace}})
    client.run("delete", RESOURCE, "--all", "--wait=true")
    client.apply({"apiVersion": "v1", "kind": "ServiceAccount",
                  "metadata": {"name": "runtime-collector", "namespace": NAMESPACE}})
    client.apply({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
        "metadata": {"name": "rc-runtime-control"},
        "rules": [
            {"apiGroups": ["observability.suse.com"], "resources": ["resourcecollections"],
             "verbs": ["get", "list", "watch"]},
            {"apiGroups": ["observability.suse.com"], "resources": ["resourcecollections/status"],
             "verbs": ["patch", "update"]},
            {"apiGroups": ["apiextensions.k8s.io"], "resources": ["customresourcedefinitions"],
             "verbs": ["get", "list", "watch"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]},
        ],
    })
    subject = {"kind": "ServiceAccount", "name": "runtime-collector", "namespace": NAMESPACE}

    def binding(name, role, namespace=None):
        obj = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding" if namespace else "ClusterRoleBinding",
            "metadata": {"name": name},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                        "kind": "ClusterRole", "name": role},
            "subjects": [subject],
        }
        if namespace:
            obj["metadata"]["namespace"] = namespace
        client.apply(obj)

    binding("rc-runtime-control", "rc-runtime-control")
    client.apply(DECLARATIONS["a"])
    capture = Capture(artifacts)
    process = None
    try:
        with __import__("tempfile").TemporaryDirectory(prefix="rc-runtime-private-") as private:
            private = Path(private)
            actor = client.service_account("runtime-collector", private)
            config = {
                "receivers": {"k8sresource": {
                    "auth_type": "kubeConfig", "cluster_name": "rc-runtime",
                    "discovery_mode": "api_groups", "cr_api_groups": {"include": []},
                    "objects": [{"name": "pods", "group": ""}],
                    "denied_objects": [{"name": "gadgets", "group": GROUP}],
                    "increment_interval": "1s", "snapshot_interval": "1m",
                    "emit_snapshot_boundaries": True, "compact_cache": args.compact,
                    "peer_sync_port": free_port(),
                }},
                "exporters": {"otlp_http/capture": {
                    "endpoint": f"http://127.0.0.1:{capture.server.server_port}",
                    "encoding": "json", "compression": "none",
                    "sending_queue": {"enabled": False},
                    "retry_on_failure": {"enabled": False},
                }},
                "service": {
                    "telemetry": {"logs": {"level": "debug"}, "metrics": {"level": "none"}},
                    "pipelines": {"logs": {
                        "receivers": ["k8sresource"], "exporters": ["otlp_http/capture"]}},
                },
            }
            config_path = private / "collector.json"
            config_path.write_text(json.dumps(config))
            command([binary, "validate", "--config", config_path])
            environment = os.environ.copy()
            environment["KUBECONFIG"] = str(actor.kubeconfig)
            with (artifacts / "collector.log").open("w") as log:
                process = subprocess.Popen([binary, "--config", config_path],
                                           env=environment, stdout=log, stderr=subprocess.STDOUT)
                original_pid = process.pid

                def feedback(name, states):
                    if process.poll() is not None:
                        raise RuntimeError(f"Collector exited: {process.returncode}")
                    obj = client.get(RESOURCE, name)
                    status = obj.get("status", {})
                    assert status.get("observedGeneration") == obj["metadata"]["generation"]
                    assert [item["state"] for item in status.get("resources", [])] == states
                    label = "-".join(states[:3])
                    if len(states) > 3:
                        label += f"-and-{len(states) - 3}-more"
                    (artifacts / f"status-{name}-{label}.json").write_text(
                        json.dumps(obj, indent=2))
                    return obj

                def state(expected):
                    records = capture.read()
                    assert_objects(records, expected)
                    return True

                def status(name, states):
                    return wait_for(f"{name} feedback {states}", lambda: feedback(name, states))

                def native_complete():
                    records = capture.read()
                    assert any((record.get("body") or {}).get("object", {}).get("kind") == "Pod"
                               for record in records)
                    assert complete_snapshots(records)
                    return True

                status("rc-test-a", ["Pending"])
                wait_for("healthy native complete snapshot", native_complete)
                record_case(report, "runtime/missing-api-native-baseline", "Pending; native complete")

                client.apply(resource_crd("widgets"))
                client.run("wait", "--for=condition=Established", "--timeout=60s",
                           f"crd/widgets.{GROUP}", timeout=70)
                for obj in objects():
                    client.apply(obj)
                status("rc-test-a", ["Blocked"])
                wait_for("no ungranted CR export", lambda: state([]))
                record_case(report, "runtime/missing-resource-permission", "Blocked; no widget export")

                client.apply({
                    "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                    "metadata": {"name": "rc-runtime-widgets"},
                    "rules": [{"apiGroups": [GROUP], "resources": ["widgets"],
                               "verbs": ["get", "list", "watch"]}],
                })
                binding("rc-runtime-widgets", "rc-runtime-widgets", "rc-team-a")
                wait_for("namespace-a read grant", lambda: actor.run(
                    "auth", "can-i", "list", f"widgets.{GROUP}",
                    "--namespace=rc-team-a", check=False).stdout.strip() == "yes")
                # Unchanged-watch backoff recovery is covered with a fake clock in Go.
                renewed_a = json.loads(json.dumps(DECLARATIONS["a"]))
                renewed_a["spec"]["resources"][0]["fieldSelector"] = "metadata.name!=backoff-probe-unmatched"
                client.apply(renewed_a)
                status("rc-test-a", ["Active"])
                only_a = ["widgets/rc-team-a/both", "widgets/rc-team-a/only-a"]
                wait_for("namespace-a selected widgets", lambda: state(only_a))
                record_case(report, "runtime/late-api-namespace-read-recovery",
                            "Active after selection edit; no restart")

                client.apply(DECLARATIONS["b"])
                status("rc-test-b", ["Blocked"])
                wait_for("healthy union child", lambda: state(only_a))
                binding("rc-runtime-widgets", "rc-runtime-widgets", "rc-team-b")
                wait_for("namespace-b read grant", lambda: actor.run(
                    "auth", "can-i", "list", f"widgets.{GROUP}",
                    "--namespace=rc-team-b", check=False).stdout.strip() == "yes")
                renewed_b = json.loads(json.dumps(DECLARATIONS["b"]))
                renewed_b["spec"]["resources"][0]["fieldSelector"] = "metadata.name!=backoff-probe-unmatched"
                client.apply(renewed_b)
                status("rc-test-b", ["Active"])
                union = only_a + ["widgets/rc-team-b/only-b"]
                wait_for("union", lambda: state(union))
                assert actor.run("auth", "can-i", "list", f"widgets.{GROUP}",
                                 "--all-namespaces", check=False).stdout.strip() == "no"
                record_case(report, "runtime/namespace-only-rbac-union", "scoped grants suffice")

                client.run("delete", RESOURCE, "rc-test-a")
                only_b = ["widgets/rc-team-a/both", "widgets/rc-team-b/only-b"]
                wait_for("contribution withdrawal", lambda: state(only_b))
                record_case(report, "runtime/remove-overlap", "surviving declaration preserved")

                client.run("label", f"widgets.{GROUP}", "only-b", "-n", "rc-team-b",
                           "monitoring-")
                wait_for("label withdrawal", lambda: state(["widgets/rc-team-a/both"]))
                client.run("label", f"widgets.{GROUP}", "only-b", "-n", "rc-team-b",
                           "monitoring=enabled")
                wait_for("label recovery", lambda: state(only_b))
                record_case(report, "runtime/label-changes", "server-filtered withdrawal/recovery")

                client.apply(resource_crd("gadgets"))
                client.run("wait", "--for=condition=Established", "--timeout=60s",
                           f"crd/gadgets.{GROUP}", timeout=70)
                client.apply({"apiVersion": f"{GROUP}/v1", "kind": "Gadget",
                              "metadata": {"name": "denied", "namespace": "rc-team-a",
                                           "labels": {"app": "alpha"}},
                              "spec": {"value": "must-not-export"}})
                client.apply(DECLARATIONS["a"])
                status("rc-test-a", ["Blocked"])
                wait_for("allowed wildcard child", lambda: state(union))
                record_case(report, "runtime/wildcard-policy", "denied child Blocked; widgets continue")

                bad = declaration("native", [select("deployments", group="apps")])
                client.apply(bad)
                status("rc-test-native", ["Blocked"])
                client.apply(resource_crd("fleets"))
                client.run("wait", "--for=condition=Established", "--timeout=60s",
                           f"crd/fleets.{GROUP}", timeout=70)
                client.apply(declaration("scope", [select("fleets", namespaces=["rc-team-a"])]))
                status("rc-test-scope", ["Blocked"])
                record_case(report, "runtime/custom-only-and-scope", "native and invalid scope Blocked")

                field = select("widgets", namespaces=["rc-team-a"])
                field["fieldSelector"] = "metadata.name=both"
                client.apply(declaration("field", [field]))
                status("rc-test-field", ["Active"])
                field["fieldSelector"] = "spec.value=initial"
                client.apply(declaration("field", [field]))
                status("rc-test-field", ["Blocked"])
                record_case(report, "runtime/field-selectors", "metadata accepted; unsupported field Blocked")

                client.apply(declaration("maximum-status", [
                    select(f"absent-{index:03d}") for index in range(32)]))
                maximum = status("rc-test-maximum-status", ["Pending"] * 32)
                entries = maximum["status"]["resources"]
                assert [item["selectionIndex"] for item in entries] == list(range(32))
                assert maximum["status"]["conditions"][0]["status"] == "False"
                assert all(len(item.get("message", "")) <= 2048 for item in entries)
                wait_for("healthy collection with maximum feedback", lambda: state(union))
                record_case(report, "runtime/maximum-status",
                            "32 current-generation results; healthy collection continues")

                def declaration_exported():
                    assert any((record.get("body") or {}).get("object", {}).get("kind") ==
                               "ResourceCollection" and
                               (record["body"]["object"].get("status", {}).get("resources"))
                               for record in capture.read())
                    return True
                wait_for("declaration/status export", declaration_exported)
                assert process.poll() is None and process.pid == original_pid
                record_case(report, "runtime/automatic-declaration-export", "status preserved; process unchanged")

                def unique_snapshot():
                    snapshots = complete_snapshots(capture.read())
                    assert snapshots
                    assert_objects(snapshots[-1], union, unique=True)
                    return True
                wait_for("complete deduplicated snapshot", unique_snapshot, timeout=90)
                record_case(report, "runtime/complete-snapshot-deduplication", "union appears once")
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        capture.close()

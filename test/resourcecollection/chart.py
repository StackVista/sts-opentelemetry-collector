"""Helm lifecycle and two-Pod checks in an owned local k3d cluster."""

import argparse
import contextlib
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import traceback
import urllib.request

from fixtures import GROUP, RESOURCE, declaration, objects, resource_crd, select
from oracle import assert_objects, identity
from run import command, digest, free_port, k3d_cluster, record_case, wait_for
from runtime import Capture
from topology import Intake, configure, native_feedback, topology_checks

NAMESPACE = "rc-lifecycle"
RELEASE = "rc-lifecycle"
COLLECTOR = RELEASE + "-k8s-resource-collector"


@contextlib.contextmanager
def forwarded(client, pod, remote_port, artifacts):
    port = free_port()
    path = artifacts / f"forward-{pod}-{remote_port}-{port}.log"
    with path.open("w") as log:
        process = subprocess.Popen([
            "kubectl", "--kubeconfig", str(client.kubeconfig), "-n", NAMESPACE,
            "port-forward", "pod/" + pod, f"{port}:{remote_port}"],
            stdout=log, stderr=subprocess.STDOUT)
        try:
            def ready():
                if process.poll() is not None:
                    raise RuntimeError("Pod port-forward exited")
                return f"Forwarding from 127.0.0.1:{port}" in path.read_text()
            wait_for("Pod port-forward", ready, timeout=20)
            yield port
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def peer_snapshot(client, pod, artifacts):
    with forwarded(client, pod, 4319, artifacts) as port:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/sync/snapshot",
                                    timeout=15) as response:
            payload = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                payload = gzip.decompress(payload)
            return [json.loads(line) for line in payload.splitlines()]


def qualify(client, args, private, artifacts, report):
    context = json.loads(client.run("config", "view", "-o", "json").stdout)
    cluster = context["clusters"][0]["name"].removeprefix("k3d-")
    if not re.fullmatch(r"resourcecollection-[0-9a-f]{10}", cluster):
        raise AssertionError("Expected the runner's owned cluster")
    network = json.loads(command([
        "docker", "network", "inspect", "k3d-" + cluster]).stdout)[0]
    gateway = network["IPAM"]["Config"][0]["Gateway"]
    image = "quay.io/sts-828-agent:lifecycle"
    command(["docker", "tag", args.agent_image, image])
    command(["k3d", "image", "import", image, "--cluster", cluster], timeout=120)
    report["image_id"] = command([
        "docker", "image", "inspect", args.agent_image, "--format", "{{.Id}}"]).stdout.strip()
    packaged = command(["helm", "package", args.chart, "--destination", artifacts])
    (artifacts / "package.log").write_text(packaged.stdout + packaged.stderr)
    archives = list(artifacts.glob("*.tgz"))
    if len(archives) != 1:
        raise AssertionError("Expected one source chart archive")
    archive = archives[0]
    report["chart_sha256"] = digest(archive)
    with tarfile.open(archive) as tar:
        member = tar.getmember(
            "suse-observability-agent/files/crds/resourcecollections.observability.suse.com.yaml")
        if tar.extractfile(member).read() != args.crd.read_bytes():
            raise AssertionError("Packaged CRD differs from the canonical artifact")

    capture = Capture(artifacts)
    intake = Intake(artifacts) if args.profile == "topology" else None
    relay = None
    relay_log = None
    try:
        relay_port = free_port()
        config = {
            "receivers": {"otlp": {"protocols": {"http": {
                "endpoint": f"0.0.0.0:{relay_port}"}}}},
            "exporters": {"otlp_http/capture": {
                "endpoint": f"http://127.0.0.1:{capture.server.server_port}",
                "encoding": "json", "compression": "none",
                "sending_queue": {"enabled": False},
                "retry_on_failure": {"enabled": False}}},
            "service": {"telemetry": {"metrics": {"level": "none"}},
                        "pipelines": {
                            signal: {"receivers": ["otlp"], "exporters": ["otlp_http/capture"]}
                            for signal in ("logs", "metrics")}},
        }
        config_path = private / "relay.json"
        config_path.write_text(json.dumps(config))
        command([args.agent_binary, "validate", "--config", config_path])
        relay_log = (artifacts / "relay.log").open("w")
        relay = subprocess.Popen([str(args.agent_binary), "--config", str(config_path)],
                                 stdout=relay_log, stderr=subprocess.STDOUT)
        values = {
            "stackstate": {"apiKey": "fixture-only", "url": "http://fixture.invalid/receiver",
                          "cluster": {"name": "rc-lifecycle", "token": "fixture-only"}},
            "global": {"imageRegistry": "quay.io"},
            "kubernetes-rbac-agent": {"enabled": False},
            **{agent: {"enabled": False} for agent in (
                "nodeAgent", "clusterAgent", "processAgent", "logsAgent", "checksAgent")},
            "otel": {
                "enabled": True, "platformHttpOtlpEndpoint": f"http://{gateway}:{relay_port}",
                "prometheusScraping": {"enabled": False},
                "integrations": {name: False for name in (
                    "suseRuntimeEnforcer", "suseAdmissionController",
                    "suseVirtualization", "suseSbomScanner", "rancherAgent")},
                "k8sResourceCollector": {
                    "image": {"repository": "sts-828-agent", "tag": "lifecycle",
                              "pullPolicy": "Never"},
                    "replicaCount": 2, "peerSync": {"port": 4319},
                    "rbac": {"useWildcard": False},
                    "crDiscovery": {"discoveryMode": "api_groups", "snapshotInterval": "1m",
                                    "apiGroups": {"include": {"unused.example.com": True}}},
                    "objects": {"pods": {"group": "", "namespaces": [NAMESPACE]}},
                    "leaderElection": {"leaseName": "rc-lifecycle"},
                    "logLevel": "info",
                },
            },
        }
        values_path = private / "values.json"
        if intake:
            configure(values, gateway, intake)
        values_path.write_text(json.dumps(values))
        helm_environment = os.environ | {
            "KUBECONFIG": str(client.kubeconfig),
            "HELM_CACHE_HOME": str(private / "helm-cache"),
            "HELM_CONFIG_HOME": str(private / "helm-config"),
            "HELM_DATA_HOME": str(private / "helm-data"),
        }
        helm_count = 0

        def helm(*arguments):
            nonlocal helm_count
            helm_count += 1
            result = command(["helm", *arguments, "--kubeconfig", client.kubeconfig],
                             env=helm_environment, timeout=180)
            (artifacts / f"helm-{helm_count}.log").write_text(result.stdout + result.stderr)
            return result

        def install():
            helm("install", RELEASE, archive, "-n", NAMESPACE, "--create-namespace",
                 "-f", values_path, "--wait", "--timeout", "150s")

        def upgrade(enabled):
            helm("upgrade", RELEASE, archive, "-n", NAMESPACE, "-f", values_path,
                 "--set", f"otel.k8sResourceCollector.resourceCollections.enabled={str(enabled).lower()}",
                 "--wait", "--timeout", "150s")

        def pods():
            result = client.run("get", "pods", "-n", NAMESPACE,
                                "-l", "app.kubernetes.io/component=k8s-resource-collector", "-o", "json")
            return json.loads(result.stdout)["items"]

        def stable():
            current = pods()
            assert len(current) == 2
            assert all(any(c["type"] == "Ready" and c["status"] == "True"
                           for c in pod["status"].get("conditions", [])) for pod in current)
            return {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in current}

        def feedback(name, state):
            obj = client.get(RESOURCE, name)
            status = obj.get("status", {})
            assert status.get("observedGeneration") == obj["metadata"]["generation"]
            assert [entry["state"] for entry in status.get("resources", [])] == [state]
            return obj

        def observed(expected, start=0):
            assert_objects(capture.read()[start:], expected)
            return True

        install()
        pod_ids = wait_for("two ready collector Pods", stable)
        crd = client.get("crd", RESOURCE)
        crd_uid = crd["metadata"]["uid"]
        assert crd["metadata"]["annotations"]["meta.helm.sh/release-name"] == RELEASE
        assert crd["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
        record_case(report, "chart-install-two-pods-and-crd-ownership", "passed")
        if intake:
            native_feedback(client)
        client.apply({
            "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
            "metadata": {"name": "rc-lifecycle-widgets"},
            "rules": [{"apiGroups": [GROUP], "resources": ["widgets"],
                       "verbs": ["get", "list", "watch"]}],
        })
        binding = {
            "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
            "metadata": {"name": "rc-lifecycle-widgets"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                        "kind": "ClusterRole", "name": "rc-lifecycle-widgets"},
            "subjects": [{"kind": "ServiceAccount", "name": COLLECTOR,
                          "namespace": NAMESPACE}],
        }
        client.apply(binding)
        deployment = client.get("deployment", COLLECTOR, NAMESPACE)
        configmap = client.get("configmap", COLLECTOR + "-config", NAMESPACE)
        request = declaration("lifecycle", [select("widgets", namespaces=["rc-team-a"],
                                                  labels={"app": "alpha"})])
        client.apply(request)
        wait_for("late API pending feedback", lambda: feedback(request["metadata"]["name"], "Pending"))
        if intake:
            wait_for("legacy topology while the custom API is pending",
                     lambda: intake.native_snapshot(), timeout=150)
            record_case(report, "pending-custom-api-keeps-native-topology", "passed")
        for namespace in ("rc-team-a", "rc-team-b"):
            client.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}})
        client.apply(resource_crd("widgets"))
        client.run("wait", "--for=condition=Established", "--timeout=60s", "crd/widgets." + GROUP)
        for obj in objects():
            client.apply(obj)
        wait_for("late API Active feedback", lambda: feedback(request["metadata"]["name"], "Active"))
        wait_for("alpha selection export", lambda: observed([
            "widgets/rc-team-a/both", "widgets/rc-team-a/only-a"]))
        request["spec"]["resources"][0]["labelSelector"]["matchLabels"]["app"] = "beta"
        client.apply(request)
        wait_for("updated Active feedback", lambda: feedback(request["metadata"]["name"], "Active"))
        expected = ["widgets/rc-team-a/cross-a"]
        wait_for("beta selection export", lambda: observed(expected))
        assert stable() == pod_ids
        assert client.get("deployment", COLLECTOR, NAMESPACE)["metadata"]["generation"] == deployment["metadata"]["generation"]
        assert client.get("configmap", COLLECTOR + "-config", NAMESPACE)["metadata"]["resourceVersion"] == configmap["metadata"]["resourceVersion"]
        record_case(report, "declarations-update-selection-without-rollout", "passed")

        def peers_ready():
            snapshots = {name: peer_snapshot(client, name, artifacts) for name in pod_ids}
            assert all("cross-a" in json.dumps(frames) for frames in snapshots.values())
            leaders = [name for name, frames in snapshots.items()
                       if frames and frames[0].get("source") == "leader"]
            assert len(leaders) == 1
            (artifacts / "peer-snapshots.json").write_text(json.dumps(snapshots, indent=2))
            return leaders[0]
        leader = wait_for("leader and replicated follower cache", peers_ready)
        follower = (pod_ids.keys() - {leader}).pop()
        lease_before = client.get("lease", "rc-lifecycle", NAMESPACE)
        record_case(report, "follower-receives-selected-object-cache", "passed")
        cursor = len(capture.read())
        client.run("delete", "clusterrolebinding", binding["metadata"]["name"], "--wait=true")
        def new_leader():
            lease = client.get("lease", "rc-lifecycle", NAMESPACE)
            assert lease["spec"]["holderIdentity"] != lease_before["spec"]["holderIdentity"]
            return lease
        # Keep the replacement from competing with the populated follower.
        node = client.get("pod", leader, NAMESPACE)["spec"]["nodeName"]
        client.run("cordon", node)
        try:
            client.run("delete", "pod", leader, "-n", NAMESPACE, "--wait=false")
            wait_for("new lease holder", new_leader)
        finally:
            client.run("uncordon", node)
        wait_for("replacement and follower ready", stable)
        wait_for("new leader sees revoked resource reads",
                 lambda: feedback(request["metadata"]["name"], "Blocked"))
        frames = peer_snapshot(client, follower, artifacts)
        assert frames[0]["source"] == "leader"
        assert "cross-a" in json.dumps(frames)
        assert stable()[follower] == pod_ids[follower]
        assert not any(identity(record) == "widgets/rc-team-a/cross-a" and
                       record["body"].get("type") == "DELETED"
                       for record in capture.read()[cursor:])
        (artifacts / "blocked-takeover-cache.json").write_text(json.dumps(frames, indent=2))
        record_case(report, "takeover-preserves-peer-data-while-read-blocked", "passed")
        if intake:
            wait_for("new producer exports native topology with blocked custom reads",
                     lambda: intake.native_snapshot(producer=follower), timeout=150)
            record_case(report, "blocked-custom-read-takeover-keeps-native-topology", "passed")
        client.apply(binding)
        wait_for("new leader resource-read recovery",
                 lambda: feedback(request["metadata"]["name"], "Active"))
        request["spec"]["resources"][0]["labelSelector"]["matchLabels"]["app"] = "alpha"
        client.apply(request)
        wait_for("new leader current-generation feedback",
                 lambda: feedback(request["metadata"]["name"], "Active"))
        wait_for("new leader alpha selection", lambda: observed([
            "widgets/rc-team-a/both", "widgets/rc-team-a/only-a"], cursor))
        record_case(report, "leader-pod-replacement-resumes-latest-generation", "passed")
        if intake:
            topology_checks(client, capture, intake, artifacts, report, stable, feedback,
                            request["metadata"]["name"], follower, peer_snapshot)
            return

        upgrade(True)
        assert client.get("crd", RESOURCE)["metadata"]["uid"] == crd_uid
        assert client.get(RESOURCE, request["metadata"]["name"])["metadata"]["generation"] == 3
        record_case(report, "same-source-upgrade-retains-crd-and-declaration", "passed")
        upgrade(False)
        assert client.get("crd", RESOURCE)["metadata"]["uid"] == crd_uid
        assert client.get(RESOURCE, request["metadata"]["name"])["metadata"]["generation"] == 3
        command_config = client.get("configmap", COLLECTOR + "-config", NAMESPACE)["data"]["config.yaml"]
        assert "resource_collections:" not in command_config
        record_case(report, "feature-disable-retains-crd-and-declarations", "passed")
        resume_cursor = len(capture.read())
        upgrade(True)
        wait_for("reenabled feedback", lambda: feedback(request["metadata"]["name"], "Active"))
        wait_for("reenabled export", lambda: observed(
            ["widgets/rc-team-a/both", "widgets/rc-team-a/only-a"], resume_cursor))
        record_case(report, "feature-reenable-resumes-collection", "passed")
        helm("rollback", RELEASE, "3", "-n", NAMESPACE, "--wait", "--timeout", "150s")
        assert "resource_collections:" not in client.get(
            "configmap", COLLECTOR + "-config", NAMESPACE)["data"]["config.yaml"]
        assert client.get("crd", RESOURCE)["metadata"]["uid"] == crd_uid
        record_case(report, "rollback-to-disabled-retains-declarations", "passed")
        upgrade(True)
        helm("uninstall", RELEASE, "-n", NAMESPACE, "--wait", "--timeout", "90s")
        assert client.get("crd", RESOURCE)["metadata"]["uid"] == crd_uid
        assert client.get(RESOURCE, request["metadata"]["name"])["metadata"]["generation"] == 3
        record_case(report, "uninstall-retains-crd-and-declarations", "passed")
        reinstall_cursor = len(capture.read())
        install()
        wait_for("reinstalled feedback", lambda: feedback(request["metadata"]["name"], "Active"))
        wait_for("reinstalled export", lambda: observed(
            ["widgets/rc-team-a/both", "widgets/rc-team-a/only-a"], reinstall_cursor))
        assert client.get("crd", RESOURCE)["metadata"]["uid"] == crd_uid
        record_case(report, "same-release-reinstall-reuses-retained-crd", "passed")
        def unique_peer_snapshots():
            for name in stable():
                frames = peer_snapshot(client, name, artifacts)
                keys = [frame["key"] for frame in frames if frame.get("type") == "object"]
                assert len(keys) == len(set(keys))
                selected = [
                    f"widgets/{frame['obj']['metadata']['namespace']}/{frame['obj']['metadata']['name']}"
                    for frame in frames if frame.get("type") == "object" and
                    (frame.get("gvr") or {}).get("Resource") == "widgets"]
                assert sorted(selected) == ["widgets/rc-team-a/both", "widgets/rc-team-a/only-a"]
            return True
        wait_for("unique selected objects in both peer snapshots", unique_peer_snapshots)
        record_case(report, "two-pod-peer-snapshots-have-no-duplicates", "passed")
        for pod in pods():
            name = pod["metadata"]["name"]
            # Telemetry binds the Pod IP; port-forward connects through loopback.
            text = client.run("get", "--raw",
                              f"/api/v1/namespaces/{NAMESPACE}/pods/{name}:8888/proxy/metrics").stdout
            (artifacts / f"metrics-{name}.txt").write_text(text)
        assert any("informer_reconciles_total" in p.read_text()
                   and 'kind="control"' in p.read_text() for p in artifacts.glob("metrics-*.txt"))
        record_case(report, "candidate-exposes-collection-reconcile-metrics", "passed")
    finally:
        with contextlib.suppress(subprocess.TimeoutExpired, OSError, json.JSONDecodeError):
            result = client.run("get", "pods", "-n", NAMESPACE, "-o", "json", check=False)
            remaining = json.loads(result.stdout)["items"] if result.returncode == 0 else []
            for pod in remaining:
                name = pod["metadata"]["name"]
                log = client.run("logs", name, "-n", NAMESPACE, "--all-containers", check=False)
                (artifacts / f"pod-{name}.log").write_text(log.stdout + log.stderr)
        if relay:
            relay.terminate()
            try:
                relay.wait(timeout=10)
            except subprocess.TimeoutExpired:
                relay.kill()
                relay.wait(timeout=5)
        if relay_log:
            relay_log.close()
        capture.close()
        if intake:
            intake.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chart", type=Path, required=True)
    parser.add_argument("--crd", type=Path, required=True)
    parser.add_argument("--agent-binary", type=Path, required=True)
    parser.add_argument("--agent-image", required=True)
    parser.add_argument("--node-image", required=True)
    parser.add_argument("--kubernetes-version", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--profile", choices=("native", "topology"), default="native")
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    if any(args.artifacts.iterdir()):
        parser.error("Artifacts directory must be empty")
    report = {"result": "failed", "profile": args.profile, "checks": [], "node_image": args.node_image,
              "crd_sha256": digest(args.crd), "agent_sha256": digest(args.agent_binary),
              "harness_sha256": {p.name: digest(p) for p in sorted(Path(__file__).parent.glob("*.py"))}}
    try:
        with tempfile.TemporaryDirectory(prefix="rc-chart-private-") as private:
            private = Path(private)
            with k3d_cluster(args, private, args.artifacts) as client:
                version = json.loads(client.run("get", "--raw=/version").stdout)
                report["server_version"] = version
                actual = f"{version['major']}.{version['minor'].rstrip('+')}"
                if actual != args.kubernetes_version:
                    raise AssertionError(f"Requested {args.kubernetes_version}; actual {actual}")
                qualify(client, args, private, args.artifacts, report)
                report["result"] = "passed"
    except (AssertionError, RuntimeError, subprocess.TimeoutExpired, OSError) as failure:
        report["error"] = f"{type(failure).__name__}: {failure}"
        (args.artifacts / "failure.txt").write_text(traceback.format_exc())
        print(f"FAIL {failure}", flush=True)
    finally:
        (args.artifacts / "report.json").write_text(json.dumps(report, indent=2))
    return 0 if report["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

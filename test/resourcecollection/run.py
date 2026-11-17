#!/usr/bin/env python3
"""Owned disposable Kubernetes admission and collector integration checks."""

import argparse
import base64
import contextlib
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import uuid
import urllib.error
import urllib.request

from fixtures import (API, DECLARATIONS, GROUP, NAMESPACE, RESOURCE,
                      admission_cases, declaration, maximum_status, select)

HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(args, *, input=None, timeout=60, check=True, env=None):
    result = subprocess.run([str(arg) for arg in args], input=input, text=True,
                            capture_output=True, timeout=timeout, check=False, env=env)
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed ({result.returncode}): {result.stderr[-4000:]}")
    return result


def wait_for(description, action, *, timeout=120):
    deadline, error = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except (RuntimeError, AssertionError, json.JSONDecodeError) as failure:
            error = failure
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {description}; last observation: {error}")


class Client:
    def __init__(self, kubeconfig):
        self.kubeconfig = Path(kubeconfig)

    def run(self, *args, data=None, check=True, timeout=40):
        return command(["kubectl", "--kubeconfig", self.kubeconfig,
                        "--request-timeout=30s", *args],
                       input=json.dumps(data) if data is not None else None,
                       check=check, timeout=timeout)

    def apply(self, obj):
        return self.run("apply", "-f", "-", data=obj)

    def get(self, resource, name=None, namespace=None):
        args = ["get", resource]
        if name:
            args.append(name)
        if namespace:
            args.extend(["-n", namespace])
        return json.loads(self.run(*args, "-o", "json").stdout)

    def service_account(self, name, directory):
        token = self.run("create", "token", name, "-n", NAMESPACE,
                         "--duration=10m").stdout.strip()
        config = json.loads(self.run("config", "view", "--raw", "-o", "json").stdout)
        config["users"] = [{"name": name, "user": {"token": token}}]
        config["contexts"][0]["context"]["user"] = name
        path = directory / f"{name}.json"
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        return Client(path)

    def patch_status(self, name, status):
        config = json.loads(self.kubeconfig.read_text())
        cluster = config["clusters"][0]["cluster"]
        authority = cluster.get("certificate-authority-data")
        context = (ssl.create_default_context(cadata=base64.b64decode(authority).decode())
                   if authority else ssl.create_default_context(
                       cafile=cluster["certificate-authority"]))
        token = config["users"][0]["user"]["token"]
        url = (cluster["server"].rstrip("/") +
               f"/apis/observability.suse.com/v1alpha1/resourcecollections/{name}/status")
        request = urllib.request.Request(
            url, data=json.dumps({"status": status}).encode(), method="PATCH",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/merge-patch+json"})
        # kubectl patch first GETs /status; the collector GETs the main resource.
        try:
            with urllib.request.urlopen(request, context=context, timeout=30) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def certificates(directory):
    openssl = shutil.which("openssl")
    if not openssl:
        raise RuntimeError("openssl is required for a private API-server fixture")
    command([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", directory / "ca.key", "-out", directory / "ca.crt",
             "-days", "1", "-subj", "/CN=resourcecollection-acceptance"])
    for name, subject, extensions in [
        ("server", "/CN=localhost",
         "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"),
        ("admin", "/CN=rc-test-admin/O=system:masters", "extendedKeyUsage=clientAuth\n"),
    ]:
        command([openssl, "req", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", directory / f"{name}.key", "-out", directory / f"{name}.csr",
                 "-subj", subject])
        extension = directory / f"{name}.ext"
        extension.write_text(extensions)
        command([openssl, "x509", "-req", "-in", directory / f"{name}.csr",
                 "-CA", directory / "ca.crt", "-CAkey", directory / "ca.key",
                 "-CAcreateserial", "-out", directory / f"{name}.crt",
                 "-days", "1", "-extfile", extension])
    command([openssl, "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048",
             "-out", directory / "service-account.key"])


@contextlib.contextmanager
def binary_cluster(args, secrets, artifacts):
    if not args.apiserver or not args.etcd:
        raise RuntimeError("binaries engine requires --apiserver and --etcd paths")
    for path in (args.apiserver, args.etcd):
        if not Path(path).is_file() or not os.access(path, os.X_OK):
            raise RuntimeError(f"Required executable unavailable: {path}")
    certificates(secrets)
    api_port, etcd_port, peer_port = free_port(), free_port(), free_port()
    processes = []
    with contextlib.ExitStack() as stack:
        try:
            for name, argv in [
                ("etcd", [args.etcd, "--name=rc-test", f"--data-dir={secrets / 'etcd'}",
                          f"--listen-client-urls=http://127.0.0.1:{etcd_port}",
                          f"--advertise-client-urls=http://127.0.0.1:{etcd_port}",
                          f"--listen-peer-urls=http://127.0.0.1:{peer_port}",
                          f"--initial-advertise-peer-urls=http://127.0.0.1:{peer_port}",
                          f"--initial-cluster=rc-test=http://127.0.0.1:{peer_port}"]),
                ("apiserver", [
                    args.apiserver, f"--etcd-servers=http://127.0.0.1:{etcd_port}",
                    "--bind-address=127.0.0.1", f"--secure-port={api_port}",
                    "--advertise-address=127.0.0.1", "--service-cluster-ip-range=10.19.0.0/24",
                    "--authorization-mode=RBAC", "--anonymous-auth=false",
                    f"--client-ca-file={secrets / 'ca.crt'}",
                    f"--tls-cert-file={secrets / 'server.crt'}",
                    f"--tls-private-key-file={secrets / 'server.key'}",
                    "--service-account-issuer=https://resourcecollection.test",
                    f"--service-account-key-file={secrets / 'service-account.key'}",
                    f"--service-account-signing-key-file={secrets / 'service-account.key'}",
                ]),
            ]:
                log = stack.enter_context((artifacts / f"{name}.log").open("w"))
                processes.append(subprocess.Popen([str(arg) for arg in argv],
                                                  stdout=log, stderr=subprocess.STDOUT))
            kubeconfig = {
                "apiVersion": "v1", "kind": "Config", "current-context": "owned",
                "clusters": [{"name": "owned", "cluster": {
                    "server": f"https://127.0.0.1:{api_port}",
                    "certificate-authority": str(secrets / "ca.crt")}}],
                "users": [{"name": "admin", "user": {
                    "client-certificate": str(secrets / "admin.crt"),
                    "client-key": str(secrets / "admin.key")}}],
                "contexts": [{"name": "owned", "context": {"cluster": "owned", "user": "admin"}}],
            }
            path = secrets / "admin.json"
            path.write_text(json.dumps(kubeconfig))
            client = Client(path)
            wait_for("private API server", lambda: client.run("get", "--raw=/readyz").stdout == "ok")
            yield client
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@contextlib.contextmanager
def kind_cluster(args, secrets, artifacts):
    if not shutil.which("kind") or not shutil.which("docker"):
        raise RuntimeError("kind and an accessible Docker daemon are required")
    if not args.node_image or "@sha256:" not in args.node_image:
        raise RuntimeError("Supply a qualified Kubernetes --node-image pinned by digest")
    command(["docker", "version"], timeout=15)
    name = f"resourcecollection-{uuid.uuid4().hex[:10]}"
    config = secrets / "admin.json"
    try:
        command(["kind", "create", "cluster", "--name", name, "--kubeconfig", config,
                 "--image", args.node_image, "--wait", "120s"], timeout=180)
        yield Client(config)
    finally:
        command(["kind", "export", "logs", "--name", name, artifacts / "kind-logs"],
                timeout=60, check=False)
        command(["kind", "delete", "cluster", "--name", name], timeout=90, check=False)


@contextlib.contextmanager
def k3d_cluster(args, secrets, artifacts):
    if not shutil.which("k3d") or not shutil.which("docker"):
        raise RuntimeError("k3d and an accessible local Docker daemon are required")
    if not args.node_image or "@sha256:" not in args.node_image:
        raise RuntimeError("Supply a Kubernetes --node-image pinned by digest")
    endpoint = command(["docker", "context", "inspect", "--format",
                        "{{.Endpoints.docker.Host}}"]).stdout.strip()
    if not endpoint.startswith("unix://"):
        raise RuntimeError("This harness requires a local Docker daemon")
    name = f"resourcecollection-{uuid.uuid4().hex[:10]}"
    path = secrets / "admin.yaml"
    environment = os.environ | {
        "KUBECONFIG": str(path), "XDG_CONFIG_HOME": str(secrets / "config")}
    try:
        result = command([
            "k3d", "cluster", "create", name, "--image", args.node_image,
            "--servers", "1", "--agents", "0", "--no-lb",
            "--api-port", f"127.0.0.1:{free_port()}",
            "--kubeconfig-update-default=false", "--kubeconfig-switch-context=false",
            "--k3s-arg", "--disable=traefik,servicelb,metrics-server,local-storage@server:0",
            "--timeout", "180s",
        ], timeout=210, env=environment)
        (artifacts / "cluster-create.log").write_text(result.stdout + result.stderr)
        path.write_text(command(["k3d", "kubeconfig", "get", name],
                                env=environment).stdout)
        path.chmod(0o600)
        yield Client(path)
    finally:
        result = command(["docker", "logs", f"k3d-{name}-server-0"],
                         timeout=30, check=False)
        (artifacts / "cluster.log").write_text(result.stdout + result.stderr)
        result = command(["k3d", "cluster", "delete", name],
                         timeout=90, env=environment)
        (artifacts / "cluster-delete.log").write_text(result.stdout + result.stderr)


def record_case(report, name, result):
    report["checks"].append({"name": name, "result": result})
    print(f"PASS {name}", flush=True)


def admission(client, args, artifacts, report):
    client.run("create", "-f", args.crd)
    client.run("wait", "--for=condition=Established", "--timeout=60s",
               "crd/resourcecollections.observability.suse.com", timeout=70)
    for name, obj, valid in admission_cases():
        result = client.run("create", "--validate=strict", "-f", "-", data=obj, check=False)
        if valid:
            if result.returncode:
                raise AssertionError(f"Valid admission {name} rejected: {result.stderr}")
            stored = client.get(RESOURCE, obj["metadata"]["name"])
            if stored["spec"] != obj["spec"]:
                raise AssertionError(f"Admission {name} unexpectedly changed/pruned the spec")
            client.run("delete", RESOURCE, obj["metadata"]["name"])
        elif not result.returncode:
            raise AssertionError(f"Invalid admission {name} accepted")
        elif not any(word in result.stderr for word in ("Invalid", "invalid", "validation")):
            raise AssertionError(f"Admission {name} failed without validation evidence: {result.stderr}")
        (artifacts / f"admission-{name}.json").write_text(json.dumps({
            "input": obj, "expected_valid": valid, "exit": result.returncode,
            "stderr": result.stderr}, indent=2))
        record_case(report, f"admission/{name}", "accepted" if valid else "rejected")
    original = copy.deepcopy(DECLARATIONS["a"])
    client.apply(original)
    stored = client.get(RESOURCE, original["metadata"]["name"])
    result = client.run("patch", RESOURCE, original["metadata"]["name"], "--type=merge",
                        "-p", '{"spec":{"resources":[]}}', check=False)
    after = client.get(RESOURCE, original["metadata"]["name"])
    if not result.returncode or stored["metadata"]["generation"] != after["metadata"]["generation"]:
        raise AssertionError("Invalid update changed the previously valid generation")
    record_case(report, "admission/invalid-update-preserves-generation", "preserved")
    unknown = copy.deepcopy(DECLARATIONS["b"])
    unknown["spec"]["resources"][0]["version"] = "v1"
    result = client.run("create", "--validate=strict", "-f", "-", data=unknown, check=False)
    if not result.returncode or "unknown field" not in result.stderr:
        raise AssertionError(f"Strict unknown-field validation failed: {result.stderr}")
    record_case(report, "admission/no-input-version", "strictly rejected")


def status_admission(client, actor, artifacts, report):
    obj = declaration("status-limits", [select("widgets") for _ in range(32)])
    client.apply(obj)
    name = obj["metadata"]["name"]
    before = client.get(RESOURCE, name)
    status = maximum_status()
    code, response = actor.patch_status(name, status)
    if code != 200:
        raise AssertionError(f"Maximum status rejected: {response}")
    stored = client.get(RESOURCE, name)
    if stored.get("status") != status or stored["spec"] != before["spec"]:
        raise AssertionError("Maximum status or original spec was changed/pruned")
    if stored["metadata"]["generation"] != before["metadata"]["generation"]:
        raise AssertionError("Status write changed the spec generation")
    record_case(report, "admission/maximum-status",
                "32 entries; 2048 Unicode characters per message; persisted exactly")
    variants = {}
    for name, mutate in [
        ("status-message-overflow", lambda s: s["resources"][0].update(message="😀" * 2049)),
        ("condition-message-overflow", lambda s: s["conditions"][0].update(message="😀" * 2049)),
        ("status-entry-overflow", lambda s: s["resources"].append(
            dict(s["resources"][0], selectionIndex=32))),
        ("status-index-overflow", lambda s: s["resources"][0].update(selectionIndex=32)),
        ("status-index-duplicate", lambda s: s["resources"][1].update(selectionIndex=0)),
    ]:
        invalid = copy.deepcopy(status)
        mutate(invalid)
        code, response = actor.patch_status(obj["metadata"]["name"], invalid)
        if code != 422 or response.get("reason") != "Invalid":
            raise AssertionError(f"{name} did not fail schema admission: {response}")
        after = client.get(RESOURCE, obj["metadata"]["name"])
        if after.get("status") != status or after["spec"] != before["spec"]:
            raise AssertionError(f"{name} changed previously accepted data")
        variants[name] = response
        record_case(report, f"admission/{name}", "rejected; valid status preserved")
    (artifacts / "status-limits.json").write_text(json.dumps({
        "accepted": stored, "rejections": variants,
        "status_utf8_bytes": len(json.dumps(status, ensure_ascii=False).encode()),
    }, indent=2, ensure_ascii=False))
    client.run("delete", RESOURCE, obj["metadata"]["name"])


def authorization(client, secrets, artifacts, report):
    client.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}})
    for name in ("integration", "namespace-only", "collector"):
        client.apply({"apiVersion": "v1", "kind": "ServiceAccount",
                      "metadata": {"name": name, "namespace": NAMESPACE}})
    roles = {
        "integration": [{"apiGroups": ["observability.suse.com"],
                         "resources": ["resourcecollections"],
                         "verbs": ["get", "list", "watch", "create", "delete", "patch", "update"]}],
        "collector": [
            {"apiGroups": ["observability.suse.com"], "resources": ["resourcecollections"],
             "verbs": ["get", "list", "watch"]},
            {"apiGroups": ["observability.suse.com"], "resources": ["resourcecollections/status"],
             "verbs": ["patch", "update"]}],
    }
    for name, rules in roles.items():
        client.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
                      "metadata": {"name": f"rc-test-{name}"}, "rules": rules})
        client.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
                      "metadata": {"name": f"rc-test-{name}"},
                      "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                                  "kind": "ClusterRole", "name": f"rc-test-{name}"},
                      "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": NAMESPACE}]})
    client.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                  "metadata": {"name": "namespace-only", "namespace": NAMESPACE},
                  "rules": roles["integration"]})
    client.apply({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                  "metadata": {"name": "namespace-only", "namespace": NAMESPACE},
                  "roleRef": {"apiGroup": "rbac.authorization.k8s.io",
                              "kind": "Role", "name": "namespace-only"},
                  "subjects": [{"kind": "ServiceAccount", "name": "namespace-only",
                                "namespace": NAMESPACE}]})
    clients = {name: client.service_account(name, secrets)
               for name in ("integration", "namespace-only", "collector")}
    target = DECLARATIONS["a"]["metadata"]["name"]
    for name, actor in clients.items():
        expected = {
            "integration": {"get": True, "list": True, "watch": True, "create": True,
                            "delete": True, "patch": True, "update": True},
            "namespace-only": {"create": False},
            "collector": {"get": True, "list": True, "watch": True, "create": False,
                          "delete": False, "patch": False, "update": False},
        }[name]
        for verb, allowed in expected.items():
            value = actor.run("auth", "can-i", verb, RESOURCE, check=False).stdout.strip()
            if value != ("yes" if allowed else "no"):
                raise AssertionError(f"Unexpected actual {name} authorization: {verb}: {value}")
        if name == "collector":
            actor.get(RESOURCE, target)
            actor.get(RESOURCE)
            code, response = actor.patch_status(target, {"observedGeneration": 1})
            if code != 200:
                raise AssertionError(f"Collector status PATCH rejected: {response}")
            status = client.get(RESOURCE, target).get("status", {})
            if status.get("observedGeneration") != 1:
                raise AssertionError("Collector status write was not persisted")
            result = actor.run("patch", RESOURCE, target, "--type=merge",
                               "-p", '{"spec":{"resources":[]}}', check=False)
        else:
            result = actor.run("create", "-f", "-",
                               data=DECLARATIONS["b"], check=False)
        if name == "integration":
            if result.returncode:
                raise AssertionError(f"Integration create denied: {result.stderr}")
            actor.run("delete", RESOURCE, DECLARATIONS["b"]["metadata"]["name"])
        elif not result.returncode or "Forbidden" not in result.stderr:
            raise AssertionError(f"{name} must receive an actual Forbidden response")
        record_case(report, f"authorization/{name}", "verified with ServiceAccount token")
    for verb in ("patch", "update"):
        result = clients["collector"].run("auth", "can-i", verb, RESOURCE,
                                          "--subresource=status", check=False)
        if result.stdout.strip() != "yes":
            raise AssertionError(f"Collector lacks {verb} /status")
    status_admission(client, clients["collector"], artifacts, report)
    client.run("delete", "clusterrolebinding", "rc-test-collector")
    code, response = clients["collector"].patch_status(target, {"observedGeneration": 1})
    if code != 403 or response.get("reason") != "Forbidden":
        raise AssertionError("Revoked status permission did not cause Forbidden")
    record_case(report, "authorization/revoked-status", "Forbidden")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["admission", "runtime"], default="admission")
    parser.add_argument("--engine", choices=["binaries", "kind", "k3d"], default="binaries")
    parser.add_argument("--crd", type=Path, required=True)
    parser.add_argument("--kubernetes-version", required=True,
                        help="Expected API-server minor, for example 1.34")
    parser.add_argument("--apiserver", type=Path)
    parser.add_argument("--etcd", type=Path)
    parser.add_argument("--node-image")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--agent-binary", type=Path)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    if not shutil.which("kubectl"):
        parser.error("kubectl is required")
    if not args.crd.is_file():
        parser.error("Provide the canonical exported CRD file")
    if args.stage == "runtime" and (
            args.engine == "binaries" or not args.agent_binary or not args.agent_binary.is_file()):
        parser.error("Runtime requires an owned kind/k3d cluster and a built --agent-binary")
    artifacts = args.artifacts or Path(tempfile.mkdtemp(prefix="resourcecollection-evidence-"))
    artifacts.mkdir(parents=True, exist_ok=True)
    if any(artifacts.iterdir()):
        parser.error("Artifacts directory must be empty")
    report = {"result": "failed", "stage": args.stage, "compact": args.compact,
              "crd_sha256": digest(args.crd), "checks": []}
    if args.agent_binary:
        report["agent_sha256"] = digest(args.agent_binary)
    report["harness_sha256"] = {
        path.name: digest(path) for path in sorted(HERE.glob("*.py"))}
    report["node_image"] = args.node_image
    print(f"Evidence: {artifacts}", flush=True)
    try:
        with tempfile.TemporaryDirectory(prefix="resourcecollection-private-") as private:
            secrets = Path(private)
            cluster = {"binaries": binary_cluster, "kind": kind_cluster,
                       "k3d": k3d_cluster}[args.engine]
            with cluster(args, secrets, artifacts) as client:
                version = json.loads(client.run("get", "--raw=/version").stdout)
                report["server_version"] = version
                actual = f"{version['major']}.{version['minor'].rstrip('+')}"
                if actual != args.kubernetes_version:
                    raise AssertionError(f"Requested {args.kubernetes_version}, server is {actual}")
                admission(client, args, artifacts, report)
                authorization(client, secrets, artifacts, report)
                if args.stage == "runtime":
                    from runtime import qualify
                    qualify(client, args, artifacts, report)
                report["result"] = "passed"
    except (AssertionError, RuntimeError, subprocess.TimeoutExpired, OSError) as failure:
        report["error"] = str(failure)
        print(f"FAIL {failure}", flush=True)
    finally:
        (artifacts / "report.json").write_text(json.dumps(report, indent=2))
    return 0 if report["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

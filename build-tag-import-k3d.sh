#!/bin/bash
docker build . -t sts-opentelemetry-collector:dev -t quay.io/sts-opentelemetry-collector:dev
kubectx k3d-crd-test
k3d image import quay.io/sts-opentelemetry-collector:dev -c crd-test
kubectl rollout restart deploy/suse-observability-agent-k8s-resource-collector -n suse-observability-agent

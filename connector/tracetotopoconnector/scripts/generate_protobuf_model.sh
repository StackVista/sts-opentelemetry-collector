#!/bin/sh

set -e && cd "$(dirname "$0")" && cd ..
echo `pwd`

OPENAPI_VERSION=$(cat topostream_version)
CHECKOUT_DIR="checkout"

rm -rf "$CHECKOUT_DIR"

# In gitlab we authenticate with the job token when cloning
if [ -z "${CI_JOB_TOKEN}" ]; then
  git clone git@gitlab.com:stackvista/platform/shared-protobuf-protocols.git "$CHECKOUT_DIR"
else
  git clone "https://stackstate-system-user:${GITLAB_READ_TOKEN}@gitlab.com/stackvista/platform/shared-protobuf-protocols.git" "$CHECKOUT_DIR"
fi

git -C "$CHECKOUT_DIR" checkout "$OPENAPI_VERSION"
cp "$CHECKOUT_DIR/topo_stream/v1/topo_stream.proto" "spec/topo_stream.proto"
rm -rf "$CHECKOUT_DIR"

go generate ./generated/topostream/model.go

#!/bin/sh

set -e && cd "$(dirname "$0")" && cd ..
echo `pwd`

PROTOBUF_VERSION=$(cat topostream_version)
CHECKOUT_DIR="checkout"

rm -rf "$CHECKOUT_DIR"

if [ -z "${GITLAB_READ_TOKEN}" ]; then
  git clone git@gitlab.com:stackvista/platform/shared-protobuf-protocols.git "$CHECKOUT_DIR"
else
  git clone "https://stackstate-system-user:${GITLAB_READ_TOKEN}@gitlab.com/stackvista/platform/shared-protobuf-protocols.git" "$CHECKOUT_DIR"
fi

git -C "$CHECKOUT_DIR" checkout "$PROTOBUF_VERSION"
cp "$CHECKOUT_DIR/topo_stream/v1/topo_stream.proto" "spec/topo_stream.proto"
rm -rf "$CHECKOUT_DIR"

export PATH="$PATH:$(go env GOPATH)/bin"
go generate ./generated/topostream/model.go

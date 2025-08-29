#!/bin/sh

set -e && cd "$(dirname "$0")" && cd ..
echo `pwd`

OPENAPI_VERSION=$(cat settings_version)
CHECKOUT_DIR="checkout"

rm -rf "$CHECKOUT_DIR"

if [ -z "${GITLAB_READ_TOKEN}" ]; then
  git clone git@gitlab.com:stackvista/platform/stackstate-openapi.git "$CHECKOUT_DIR"
else
  git clone "https://stackstate-system-user:${GITLAB_READ_TOKEN}@gitlab.com/stackvista/platform/stackstate-openapi.git" "$CHECKOUT_DIR"
fi

git -C "$CHECKOUT_DIR" checkout "$OPENAPI_VERSION"
cp "$CHECKOUT_DIR/spec_settings/openapi.yaml" "spec/settings.yaml"
rm -rf "$CHECKOUT_DIR"

go generate ./generated/settings/model.go

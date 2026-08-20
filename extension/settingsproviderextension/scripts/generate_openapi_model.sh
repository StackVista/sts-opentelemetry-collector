#!/bin/sh

set -e && cd "$(dirname "$0")" && cd ..
echo `pwd`

OPENAPI_VERSION=$(cat settings_version)
CHECKOUT_DIR="checkout"

rm -rf "$CHECKOUT_DIR"

git clone https://github.com/StackVista/stackstate-openapi.git "$CHECKOUT_DIR"

git -C "$CHECKOUT_DIR" checkout "$OPENAPI_VERSION"
cp "$CHECKOUT_DIR/spec_settings/openapi.yaml" "spec/openapi.yaml"
cp "$CHECKOUT_DIR/spec_settings/settings.yaml" "spec/settings.yaml"
rm -rf "$CHECKOUT_DIR"

go generate ./generated/settingsproto/... ./generated/settingsschema/...

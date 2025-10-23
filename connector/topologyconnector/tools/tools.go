//go:build tools
// +build tools

package main

import (
	_ "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)

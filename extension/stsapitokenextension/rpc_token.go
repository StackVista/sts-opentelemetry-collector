package stsapitokenextension

import (
	"context"

	"google.golang.org/grpc/credentials"
)

var _ credentials.PerRPCCredentials = (*PerRPCToken)(nil)

// PerRPCToken is a gRPC credentials.PerRPCCredentials implementation that returns an 'authorization' header.
type PerRPCToken struct {
	metadata map[string]string
}

// GetRequestMetadata returns the request metadata to be used with the RPC.
func (c *PerRPCToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return c.metadata, nil
}

func (c *PerRPCToken) RequireTransportSecurity() bool {
	return true
}

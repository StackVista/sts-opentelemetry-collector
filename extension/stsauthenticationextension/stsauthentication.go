package stsauthenticationextension

import (
	"context"
	"errors"

	stsauth "github.com/stackvista/sts-opentelemetry-collector/common/auth"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/auth"
	"go.uber.org/zap"
)

var _ auth.Server = (*StsAuthentication)(nil)

type StsAuthentication struct {
	FixedTenants map[string]string
}

func newStsAuthenticator(cfg *Config, logger *zap.Logger) *StsAuthentication {
	tenants := make(map[string]string)
	for k, v := range cfg.Tenants {
		tenants[string(k)] = v
	}

	return &StsAuthentication{FixedTenants: tenants}
}

// Start of StsAuthentication does nothing.
func (b *StsAuthentication) Start(ctx context.Context, _ component.Host) error {
	return nil
}

// Shutdown stops the authenticator.
func (b *StsAuthentication) Shutdown(context.Context) error {
	return nil
}

// Authenticate checks whether the given context contains valid auth data.
func (a *StsAuthentication) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	head, ok := headers[stsauth.StsAPIKeyHeader]
	if !ok || len(head) == 0 {
		return ctx, errors.New("No authentication found in request")
	}
	token := head[0]

	newCtx, err := a.validateKey(ctx, token)
	if err != nil {
		return ctx, err
	}
	return newCtx, nil
}

func (a *StsAuthentication) validateKey(ctx context.Context, incomingKey string) (context.Context, error) {
	if _, ok := a.FixedTenants[incomingKey]; !ok {
		return ctx, errors.New("Unknown StackState API Token")
	}

	ctx = context.WithValue(ctx, stsauth.StsAPIKeyHeader, incomingKey)
	ctx = context.WithValue(ctx, stsauth.StsTenantContextKey, a.FixedTenants[incomingKey])
	return ctx, nil
}

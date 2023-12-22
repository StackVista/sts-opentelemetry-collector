package stsauthenticationextension

import (
	"context"
	"testing"

	stsauth "github.com/stackvista/sts-opentelemetry-collector/common/auth"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

func TestShouldNotAuthenticateWithMissingHeader(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Tenants = map[configopaque.String]string{"somerandometoken": "tenant"}
	ext, err := createExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.NoError(t, err)
	_, err = ext.(*StsAuthentication).Authenticate(context.Background(), map[string][]string{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "No authentication found in request")
}

func TestShouldNotAuthenticateWithInvalidHeader(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Tenants = map[configopaque.String]string{"somerandometoken": "tenant"}
	ext, err := createExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.NoError(t, err)
	_, err = ext.(*StsAuthentication).Authenticate(context.Background(), map[string][]string{stsauth.StsAPIKeyHeader: {"invalid"}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Unknown StackState API Token")
}

func TestShouldAuthenticateWithValidHeader(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Tenants = map[configopaque.String]string{"somerandometoken": "tenant"}
	ext, err := createExtension(context.Background(), extensiontest.NewNopCreateSettings(), cfg)
	assert.NoError(t, err)
	ctx, err := ext.(*StsAuthentication).Authenticate(context.Background(), map[string][]string{stsauth.StsAPIKeyHeader: {"somerandometoken"}})
	assert.NoError(t, err)
	assert.Equal(t, "tenant", ctx.Value(stsauth.StsTenantContextKey))
	assert.Equal(t, "somerandometoken", ctx.Value(stsauth.StsAPIKeyHeader))
}

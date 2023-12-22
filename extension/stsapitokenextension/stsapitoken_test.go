package stsapitokenextension

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	stsauth "github.com/stackvista/sts-opentelemetry-collector/common/auth"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"

	"go.uber.org/zap/zaptest"
)

func TestPerRPCAuth(t *testing.T) {
	metadata := map[string]string{
		stsauth.StsAPIKeyHeader: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
	}

	// test meta data is properly
	perRPCAuth := &PerRPCToken{metadata: metadata}
	md, err := perRPCAuth.GetRequestMetadata(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, md, metadata)

	// always true
	ok := perRPCAuth.RequireTransportSecurity()
	assert.True(t, ok)
}

type mockRoundTripper struct{}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{StatusCode: http.StatusOK, Header: map[string][]string{}}
	for k, v := range req.Header {
		resp.Header.Set(k, v[0])
	}
	return resp, nil
}

func TestStSAPITokenAuthenticatorHttp(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.APIToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

	bauth := newStsAPITokenAuth(cfg, nil)
	assert.NotNil(t, bauth)

	base := &mockRoundTripper{}
	c, err := bauth.RoundTripper(base)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	request := &http.Request{Method: "Get"}
	resp, err := c.RoundTrip(request)
	assert.NoError(t, err)
	authHeaderValue := resp.Header.Get(stsauth.StsAPIKeyHeader)
	assert.Equal(t, authHeaderValue, string(cfg.APIToken))

}

func TestStSAPITokenAuthenticator(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.APIToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

	bauth := newStsAPITokenAuth(cfg, nil)
	assert.NotNil(t, bauth)

	assert.Nil(t, bauth.Start(context.Background(), componenttest.NewNopHost()))
	credential, err := bauth.PerRPCCredentials()

	assert.NoError(t, err)
	assert.NotNil(t, credential)

	md, err := credential.GetRequestMetadata(context.Background())
	expectedMd := map[string]string{
		stsauth.StsAPIKeyHeader: string(cfg.APIToken),
	}
	assert.Equal(t, md, expectedMd)
	assert.NoError(t, err)
	assert.True(t, credential.RequireTransportSecurity())

	roundTripper, _ := bauth.RoundTripper(&mockRoundTripper{})
	orgHeaders := http.Header{
		"Foo": {"bar"},
	}
	expectedHeaders := http.Header{
		"Foo":                   {"bar"},
		stsauth.StsAPIKeyHeader: {bauth.token()},
	}

	resp, err := roundTripper.RoundTrip(&http.Request{Header: orgHeaders})
	assert.NoError(t, err)
	assert.Equal(t, expectedHeaders, resp.Header)
	assert.Nil(t, bauth.Shutdown(context.Background()))
}

func TestStSAPITokenStartWatchStop(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Filename = "test.token"

	bauth := newStsAPITokenAuth(cfg, zaptest.NewLogger(t))
	assert.NotNil(t, bauth)

	assert.Nil(t, bauth.Start(context.Background(), componenttest.NewNopHost()))
	assert.Error(t, bauth.Start(context.Background(), componenttest.NewNopHost()))

	credential, err := bauth.PerRPCCredentials()
	assert.NoError(t, err)
	assert.NotNil(t, credential)

	token, err := os.ReadFile(bauth.filename)
	assert.NoError(t, err)

	tokenStr := string(token)
	md, err := credential.GetRequestMetadata(context.Background())
	expectedMd := map[string]string{
		stsauth.StsAPIKeyHeader: tokenStr,
	}
	assert.Equal(t, md, expectedMd)
	assert.NoError(t, err)
	assert.True(t, credential.RequireTransportSecurity())

	// change file content once
	assert.Nil(t, os.WriteFile(bauth.filename, []byte(fmt.Sprintf("%stest", token)), 0600))
	time.Sleep(5 * time.Second)
	credential, _ = bauth.PerRPCCredentials()
	md, err = credential.GetRequestMetadata(context.Background())
	expectedMd[stsauth.StsAPIKeyHeader] = tokenStr + "test"
	assert.Equal(t, md, expectedMd)
	assert.NoError(t, err)

	// change file content back
	assert.Nil(t, os.WriteFile(bauth.filename, token, 0600))
	time.Sleep(5 * time.Second)
	credential, _ = bauth.PerRPCCredentials()
	md, err = credential.GetRequestMetadata(context.Background())
	expectedMd[stsauth.StsAPIKeyHeader] = tokenStr
	time.Sleep(5 * time.Second)
	assert.Equal(t, md, expectedMd)
	assert.NoError(t, err)

	assert.Nil(t, bauth.Shutdown(context.Background()))
	assert.Nil(t, bauth.shutdownCh)
}

func TestStSAPITokenTokenFileContentUpdate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Filename = "test.token"

	bauth := newStsAPITokenAuth(cfg, zaptest.NewLogger(t))
	assert.NotNil(t, bauth)

	assert.Nil(t, bauth.Start(context.Background(), componenttest.NewNopHost()))
	assert.Error(t, bauth.Start(context.Background(), componenttest.NewNopHost()))

	token, err := os.ReadFile(bauth.filename)
	assert.NoError(t, err)

	base := &mockRoundTripper{}
	rt, err := bauth.RoundTripper(base)
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	request := &http.Request{Method: "Get"}
	resp, err := rt.RoundTrip(request)
	assert.NoError(t, err)
	authHeaderValue := resp.Header.Get(stsauth.StsAPIKeyHeader)
	assert.Equal(t, authHeaderValue, string(token))

	// change file content once
	assert.Nil(t, os.WriteFile(bauth.filename, []byte(fmt.Sprintf("%stest", token)), 0600))
	time.Sleep(5 * time.Second)

	tokenNew, err := os.ReadFile(bauth.filename)
	assert.NoError(t, err)

	// check if request is updated with the new token
	request = &http.Request{Method: "Get"}
	resp, err = rt.RoundTrip(request)
	assert.NoError(t, err)
	authHeaderValue = resp.Header.Get(stsauth.StsAPIKeyHeader)
	assert.Equal(t, authHeaderValue, string(tokenNew))

	// change file content back
	assert.Nil(t, os.WriteFile(bauth.filename, token, 0600))
	time.Sleep(5 * time.Second)

	// check if request is updated with the old token
	request = &http.Request{Method: "Get"}
	resp, err = rt.RoundTrip(request)
	assert.NoError(t, err)
	authHeaderValue = resp.Header.Get(stsauth.StsAPIKeyHeader)
	assert.Equal(t, authHeaderValue, string(token))
}

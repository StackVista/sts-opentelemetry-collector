package servicetokenauthextension_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/extension/servicetokenauthextension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

const KEY = "key"

func TestExtension_NoHeader(t *testing.T) {
	ext, err := servicetokenauthextension.NewServerAuthExtension(
		&servicetokenauthextension.Config{
			Endpoint: &servicetokenauthextension.EndpointSettings{
				URL: "http://localhost:8091/authorize",
			},
			Cache: &servicetokenauthextension.CacheSettings{
				ValidSize:   2,
				ValidTTL:    30 * time.Second,
				InvalidSize: 3,
			},
			Schema: "StackState",
		})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{})
	assert.Equal(t, servicetokenauthextension.ErrNoAuth, err)
}

func TestExtension_AuthServerUnavailable(t *testing.T) {
	ext, err := servicetokenauthextension.NewServerAuthExtension(&servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: "http://localhost:1/authorize",
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 3,
		},
		Schema: "StackState",
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState key"}})
	assert.Equal(t, servicetokenauthextension.ErrAuthServerUnavailable, err)
}

func TestExtension_InvalidKey(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		token := req.Header.Get("sts-api-key")
		if token == KEY {
			res.WriteHeader(403)
			return
		}
		// Default to 204 to avoid test failures for unexpected requests
		res.WriteHeader(204)
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(&servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: testServer.URL,
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 3,
		},
		Schema: "StackState",
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState key"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)
}

//nolint:dupl
func TestExtension_Authorized(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		token := req.Header.Get("sts-api-key")
		if token == KEY {
			res.WriteHeader(204)
			return
		}
		res.WriteHeader(403)
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(&servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: testServer.URL,
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 3,
		},
		Schema: "StackState",
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState key"}})
	require.NoError(t, err)
}

func TestExtension_WrongSchema(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		token := req.Header.Get("sts-api-key")
		if token == KEY {
			res.WriteHeader(204)
			return
		}
		res.WriteHeader(403)
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(&servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: testServer.URL,
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 3,
		},
		Schema: "StackState",
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState_wrong key"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)

	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"key"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)

	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)
}

//nolint:dupl
func TestExtension_AuthorizedWithCamelcaseHeader(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		token := req.Header.Get("sts-api-key")
		if token == KEY {
			res.WriteHeader(204)
			return
		}
		res.WriteHeader(403)
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(
		&servicetokenauthextension.Config{
			Endpoint: &servicetokenauthextension.EndpointSettings{
				URL: testServer.URL,
			},
			Cache: &servicetokenauthextension.CacheSettings{
				ValidSize:   2,
				ValidTTL:    30 * time.Second,
				InvalidSize: 3,
			},
			Schema: "StackState",
		})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"StackState key"}})
	require.NoError(t, err)
}

func TestExtension_ValidKeysShouldBeCached(t *testing.T) {
	var requestCounter = 0
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, _ *http.Request) {
		//nolint:forbidigo
		println(requestCounter)
		switch requestCounter {
		case 0:
			res.WriteHeader(204)
		case 1:
			res.WriteHeader(403)
		default:
			t.Fatal("The second request should be cached so it shouldn't hit the server")
		}

		requestCounter++
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(
		&servicetokenauthextension.Config{
			Endpoint: &servicetokenauthextension.EndpointSettings{
				URL: testServer.URL,
			},
			Cache: &servicetokenauthextension.CacheSettings{
				ValidSize:   2,
				ValidTTL:    30 * time.Second,
				InvalidSize: 3,
			},
			Schema: "StackState",
		})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"StackState key"}})
	require.NoError(t, err)
	// it should be loaded from the cache, it is the same cache as in the previous request
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"StackState key"}})
	require.NoError(t, err)
	// send one more request, but with a different key, it shouldn't hit the cache
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState key_new"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)
}

func TestExtension_InvalidKeyShouldBeCached(t *testing.T) {
	var requestCounter = 0
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, _ *http.Request) {
		//nolint:forbidigo
		println(requestCounter)
		switch requestCounter {
		case 0:
			res.WriteHeader(503)
		case 1:
			res.WriteHeader(403)
		default:
			t.Fatal("The second request should be cached so it shouldn't hit the server")
		}
		requestCounter++
	}))

	ext, err := servicetokenauthextension.NewServerAuthExtension(&servicetokenauthextension.Config{
		Endpoint: &servicetokenauthextension.EndpointSettings{
			URL: testServer.URL,
		},
		Cache: &servicetokenauthextension.CacheSettings{
			ValidSize:   2,
			ValidTTL:    30 * time.Second,
			InvalidSize: 1,
		},
		Schema: "StackState",
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	// server is broken and returns 503, it shouldn't be cached
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"StackState invalid_key"}})
	assert.Equal(t, servicetokenauthextension.ErrInternal, err)
	// The server is fixed so the response should be cached
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"StackState invalid_key"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)
	// the previous request is cached so it shouldn't hit the server
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"StackState invalid_key"}})
	assert.Equal(t, servicetokenauthextension.ErrForbidden, err)
}

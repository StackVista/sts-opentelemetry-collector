package ingestionapikeyauthextension

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExtension_NoHeader(t *testing.T) {
	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: "http://localhost:8091/authorize",
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{})
	assert.Equal(t, errNoAuth, err)
}

func TestExtension_AuthServerUnavailable(t *testing.T) {
	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: "http://localhost:1/authorize",
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"key"}})
	assert.Equal(t, errAuthServerUnavailable, err)
}

func TestExtension_InvalidKey(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(403)
	}))

	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: testServer.URL,
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"key"}})
	assert.Equal(t, errForbidden, err)
}

func TestExtension_Authorized(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(204)
	}))

	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: testServer.URL,
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"key"}})
	require.NoError(t, err)
}

func TestExtension_AuthorizedWithCamelcaseHeader(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(204)
	}))

	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: testServer.URL,
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"key"}})
	require.NoError(t, err)
}

func TestExtension_ValidKeysShouldBeCached(t *testing.T) {
	var requestCounter = 0
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		println(requestCounter)
		if requestCounter == 0 {
			res.WriteHeader(204)
		} else if requestCounter == 1 {
			res.WriteHeader(403)
		} else {
			t.Fatal("The second request should be cached so it shouldn't hit the server")
		}
		requestCounter += 1
	}))

	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: testServer.URL,
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"key"}})
	require.NoError(t, err)
	//it should be loaded from the cache, it is the same cache as in the previous request
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"key"}})
	require.NoError(t, err)
	//send one more request, but with a different key, it shouldn't hit the cache
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"key_new"}})
	assert.Equal(t, errForbidden, err)
}

func TestExtension_InvalidKeyShouldBeCached(t *testing.T) {
	var requestCounter = 0
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		println(requestCounter)
		if requestCounter == 0 {
			res.WriteHeader(503)
		} else if requestCounter == 1 {
			res.WriteHeader(403)
		} else {
			t.Fatal("The second request should be cached so it shouldn't hit the server")
		}
		requestCounter += 1
	}))

	ext, err := newServerAuthExtension(&Config{
		Endpoint: &EndpointSettings{
			Url: testServer.URL,
		},
		Cache: &CacheSettings{
			ValidSize:   2,
			ValidTtl:    30 * time.Second,
			InvalidSize: 1,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ext.Start(context.Background(), componenttest.NewNopHost()))
	//server is broken and returns 503, it shouldn't be cached
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"invalid_key"}})
	assert.Equal(t, errInternal, err)
	//The server is fixed so teh response should be cached
	_, err = ext.Authenticate(context.Background(), map[string][]string{"Authorization": {"invalid_key"}})
	assert.Equal(t, errForbidden, err)
	//the previous request is cached so it shouldn't hit the server
	_, err = ext.Authenticate(context.Background(), map[string][]string{"authorization": {"invalid_key"}})
	assert.Equal(t, errForbidden, err)
}

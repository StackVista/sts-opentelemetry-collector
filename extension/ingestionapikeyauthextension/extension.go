package ingestionapikeyauthextension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/auth"
	"log"
	"net/http"
	"regexp"
	"time"
)

var (
	errNoAuth                = errors.New("missing Authorization header")
	errInternal              = errors.New("internal error")
	errAuthServerUnavailable = errors.New("auth server unavailable")
	errForbidden             = errors.New("forbidden")

	tokenRegex = regexp.MustCompile(`(\w+) (.*)$`)
)

type extensionContext struct {
	config     *Config
	httpClient http.Client
	//we have two caches one for invalid keys, it maps auth key to nothing now but in the future we can map it to tenant ID
	//it is LRU cache with TTL to delete unused keys after some time, the key should be always expired even if is constantly
	//used, otherwise a key ma be invalidated and not deleted from the cache (because is still used).
	validKeysCache *expirable.LRU[string, string]
	//the cache stores last invalid keys to reject only valid collectors but without updated API key (e.g. expired)
	//the cache can't prevent DoS or brute force attacks, it should be prevented on LB or API Gateway
	//the cache maps maps auth key to an error but only "non transient" errors like Forbidden and shouldn't be used for transient
	//issues (like authorization service unavailable).
	invalidKeysCache *expirable.LRU[string, error]
}

func newServerAuthExtension(cfg *Config) (auth.Server, error) {
	exCtx := extensionContext{
		config:           cfg,
		validKeysCache:   expirable.NewLRU[string, string](cfg.Cache.ValidSize, nil, cfg.Cache.ValidTtl),
		invalidKeysCache: expirable.NewLRU[string, error](cfg.Cache.InvalidSize, nil, 0),
	}
	return auth.NewServer(
		auth.WithServerStart(exCtx.serverStart),
		auth.WithServerAuthenticate(exCtx.authenticate),
	), nil
}

func (exCtx *extensionContext) serverStart(context.Context, component.Host) error {
	httpClient := http.Client{
		Timeout: 5 * time.Second, // TODO configure timeout
	}

	exCtx.httpClient = httpClient

	return nil
}

func (exCtx *extensionContext) authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	authorizationHeader := getAuthHeader(headers)
	if authorizationHeader == "" {
		return ctx, errNoAuth
	}

	// checks schema
	matches := tokenRegex.FindStringSubmatch(authorizationHeader)
	if len(matches) != 3 {
		return ctx, errForbidden
	}
	if matches[1] != exCtx.config.Schema {
		return ctx, errForbidden
	}

	err := checkAuthorizationHeaderUseCache(matches[2], exCtx)
	if err != nil {
		return ctx, err
	}

	cl := client.FromContext(ctx)
	return client.NewContext(ctx, cl), nil
}

var authHeaders = [2]string{"authorization", "Authorization"}

// Extract value of "Authorization" header, empty string - the header is missing.
func getAuthHeader(headers map[string][]string) string {
	for _, authHeaderName := range authHeaders {
		authHeader, ok := headers[authHeaderName]

		if ok && len(authHeader) > 0 {
			return authHeader[0]
		}
	}

	return ""
}

// Check if an Ingestion API Key is inside caches otherwise use a remote server to authorize it
func checkAuthorizationHeaderUseCache(authorizationHeader string, exCtx *extensionContext) error {
	// check if the key is stored in "validKeysCache" cache, so we know the Key is valid.
	_, ok := exCtx.validKeysCache.Get(authorizationHeader)
	if ok {
		return nil
	}

	// check if the key is stored in "invalidKeysCache" cache, so we know the Key is invalid, reject it immediately
	er, ok := exCtx.invalidKeysCache.Get(authorizationHeader)
	if ok {
		return er
	}

	// otherwise use a remote server to authorize the key
	return checkAuthorizationHeader(authorizationHeader, exCtx)
}

type AuthorizeRequestBody struct {
	ApiKey string `json:"apiKey"`
}

// Authorizes an Ingestion API Key (value of Authorization header) with the remote authorization server.
// The function stores the result (valid keys but also non-transient errors) in the cache.
func checkAuthorizationHeader(authorizationHeader string, exCtx *extensionContext) error {
	log.Println("Sending authorization request...")
	request := AuthorizeRequestBody{
		ApiKey: authorizationHeader,
	}
	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Print("Can't encode api request to JSON ", err)
		return errInternal //it shouldn't happen, something is wrong with the implementation
	}

	req, err := http.NewRequest(http.MethodPost, exCtx.config.Endpoint.Url, bytes.NewReader(jsonData))
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		log.Print("Can't create authorization request ", err)
		return errInternal
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print("Authorization endpoint returned an error ", err)
		return errAuthServerUnavailable
	}

	if res.StatusCode == 403 {
		exCtx.invalidKeysCache.Add(authorizationHeader, errForbidden)
		return errForbidden
	}

	if res.StatusCode == 204 {
		exCtx.validKeysCache.Add(authorizationHeader, "") //In future we can store tenant ID in the cache
		return nil
	}

	return errInternal
}

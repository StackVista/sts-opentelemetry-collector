package servicetokenauthextension

import (
	"context"
	"errors"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	stsauth "github.com/stackvista/sts-opentelemetry-collector/common/auth"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
)

var (
	ErrNoAuth                = errors.New("missing Authorization header")
	ErrInternal              = errors.New("internal error")
	ErrAuthServerUnavailable = errors.New("auth server unavailable")
	ErrForbidden             = errors.New("forbidden")

	tokenRegex = regexp.MustCompile(`(\w+) (.*)$`)
)

type serviceTokenAuthServer struct {
	config     *Config
	httpClient http.Client
	// we have two caches one for invalid keys, it maps auth key to nothing now but in the future
	// we can map it to tenant ID
	// it is LRU cache with TTL to delete unused keys after some time, the key should be always expired even
	// if it is constantly used, otherwise a key ma be invalidated and not deleted from the cache (because is still used).
	validKeysCache *expirable.LRU[string, string]
	// the cache stores last invalid keys to reject only valid collectors but without updated API key (e.g. expired)
	// the cache can't prevent DoS or brute force attacks, it should be prevented on LB or API Gateway
	// the cache maps maps auth key to an error but only "non transient" errors like Forbidden and shouldn't be
	// used for transient issues (like authorization service unavailable).
	invalidKeysCache *expirable.LRU[string, error]
}

func NewServiceTokenAuth(cfg *Config) (extension.Extension, error) {
	return &serviceTokenAuthServer{
		config:           cfg,
		validKeysCache:   expirable.NewLRU[string, string](cfg.Cache.ValidSize, nil, cfg.Cache.ValidTTL),
		invalidKeysCache: expirable.NewLRU[string, error](cfg.Cache.InvalidSize, nil, 0),
	}, nil
}

func (s *serviceTokenAuthServer) Start(_ context.Context, _ component.Host) error {
	s.httpClient = http.Client{
		Timeout: 5 * time.Second,
	}
	return nil
}

func (s *serviceTokenAuthServer) Shutdown(_ context.Context) error {
	return nil
}

func (s *serviceTokenAuthServer) Authenticate(
	ctx context.Context,
	headers map[string][]string,
) (context.Context, error) {
	authorizationHeader := getAuthHeader(headers)
	if authorizationHeader == "" {
		return ctx, ErrNoAuth
	}

	matches := tokenRegex.FindStringSubmatch(authorizationHeader)
	if len(matches) != 3 {
		return ctx, ErrForbidden
	}
	if matches[1] != s.config.Schema {
		return ctx, ErrForbidden
	}

	err := s.checkAuthorizationHeaderUseCache(matches[2])
	if err != nil {
		return ctx, err
	}

	cl := client.FromContext(ctx)
	cl.Auth = &stsauth.Data{
		APIKey: matches[2],
	}
	return client.NewContext(ctx, cl), nil
}

func getAuthHeader(headers map[string][]string) string {
	authHeaders := [2]string{"authorization", "Authorization"}

	for _, authHeaderName := range authHeaders {
		authHeader, ok := headers[authHeaderName]

		if ok && len(authHeader) > 0 {
			return authHeader[0]
		}
	}

	return ""
}

func (s *serviceTokenAuthServer) checkAuthorizationHeaderUseCache(authorizationHeader string) error {
	_, ok := s.validKeysCache.Get(authorizationHeader)
	if ok {
		return nil
	}

	er, ok := s.invalidKeysCache.Get(authorizationHeader)
	if ok {
		return er
	}

	return s.checkAuthorizationHeader(authorizationHeader)
}

func (s *serviceTokenAuthServer) checkAuthorizationHeader(token string) error {
	headerSample := token[max(0, len(token)-4):]
	log.Printf("Sending authorization request for ...%s\n", headerSample)

	req, err := http.NewRequest(http.MethodGet, s.config.Endpoint.URL, nil)
	if err != nil {
		log.Print("Can't create authorization request ", err)
		return ErrInternal
	}

	req.Header.Add("sts-api-key", token)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print("Authorization endpoint returned an error ", err)
		return ErrAuthServerUnavailable
	}

	log.Printf("Result for ...%s: %d\n", headerSample, res.StatusCode)
	if res.StatusCode == 403 {
		s.invalidKeysCache.Add(token, ErrForbidden)
		return ErrForbidden
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		s.validKeysCache.Add(token, "")
		return nil
	}

	return ErrInternal
}

var _ extensionauth.Server = (*serviceTokenAuthServer)(nil)

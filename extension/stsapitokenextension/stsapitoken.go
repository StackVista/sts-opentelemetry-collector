package stsapitokenextension

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/fsnotify/fsnotify"
	stsauth "github.com/stackvista/sts-opentelemetry-collector/common/auth"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

var (
	_ auth.Client = (*StsAPITokenAuth)(nil)
)

type StsAPITokenAuth struct {
	muTokenString sync.RWMutex
	tokenString   string

	shutdownCh chan struct{}

	filename string
	logger   *zap.Logger
}

var _ auth.Client = (*StsAPITokenAuth)(nil)

func newStsAPITokenAuth(cfg *Config, logger *zap.Logger) *StsAPITokenAuth {
	if cfg.Filename != "" && cfg.APIToken != "" {
		logger.Warn("a filename is specified. Configured token is ignored!")
	}
	return &StsAPITokenAuth{
		tokenString: string(cfg.APIToken),
		filename:    cfg.Filename,
		logger:      logger,
	}
}

// Start of StsAPITokenAuth does nothing and returns nil if no filename
// is specified. Otherwise a routine is started to monitor the file containing
// the token to be transferred.
func (b *StsAPITokenAuth) Start(ctx context.Context, _ component.Host) error {
	if b.filename == "" {
		return nil
	}

	if b.shutdownCh != nil {
		return fmt.Errorf("StS API Token file monitoring is already running")
	}

	// Read file once
	b.refreshToken()

	b.shutdownCh = make(chan struct{})

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	// start file watcher
	go b.startWatcher(ctx, watcher)

	return watcher.Add(b.filename)
}

func (b *StsAPITokenAuth) startWatcher(ctx context.Context, watcher *fsnotify.Watcher) {
	defer watcher.Close()
	for {
		select {
		case _, ok := <-b.shutdownCh:
			_ = ok
			return
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				continue
			}
			// NOTE: k8s configmaps uses symlinks, we need this workaround.
			// original configmap file is removed.
			// SEE: https://martensson.io/go-fsnotify-and-kubernetes-configmaps/
			if event.Op == fsnotify.Remove || event.Op == fsnotify.Chmod {
				// remove the watcher since the file is removed
				if err := watcher.Remove(event.Name); err != nil {
					b.logger.Error(err.Error())
				}
				// add a new watcher pointing to the new symlink/file
				if err := watcher.Add(b.filename); err != nil {
					b.logger.Error(err.Error())
				}
				b.refreshToken()
			}
			// also allow normal files to be modified and reloaded.
			if event.Op == fsnotify.Write {
				b.refreshToken()
			}
		}
	}
}

func (b *StsAPITokenAuth) refreshToken() {
	b.logger.Info("Refresh StS API Token", zap.String("filename", b.filename))
	token, err := os.ReadFile(b.filename)
	if err != nil {
		b.logger.Error(err.Error())
		return
	}
	b.muTokenString.Lock()
	b.tokenString = string(token)
	b.muTokenString.Unlock()
}

// Shutdown of BearerTokenAuth does nothing and returns nil
func (b *StsAPITokenAuth) Shutdown(_ context.Context) error {
	if b.filename == "" {
		return nil
	}

	if b.shutdownCh == nil {
		return fmt.Errorf("bearerToken file monitoring is not running")
	}
	b.shutdownCh <- struct{}{}
	close(b.shutdownCh)
	b.shutdownCh = nil
	return nil
}

// PerRPCCredentials returns PerRPCAuth an implementation of credentials.PerRPCCredentials that
func (b *StsAPITokenAuth) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	return &PerRPCToken{
		metadata: map[string]string{stsauth.StsAPIKeyHeader: b.token()},
	}, nil
}

func (b *StsAPITokenAuth) token() string {
	b.muTokenString.RLock()
	defer b.muTokenString.RUnlock()
	return fmt.Sprintf("%s", b.tokenString)
}

// RoundTripper is not implemented by BearerTokenAuth
func (b *StsAPITokenAuth) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	return &StsAPITokenRoundTripper{
		baseTransport:  base,
		apiTokenFunc:   b.token,
		apiTokenHeader: stsauth.StsAPIKeyHeader,
	}, nil
}

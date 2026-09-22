package stsk8slogsexporter

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const failureConfiguration = "configuration"
const failureAttemptTimeout = "attempt_timeout"

type exportFailure struct {
	reason string
	status int
	cause  error
}

func (e *exportFailure) Error() string {
	if e.status != 0 {
		return fmt.Sprintf("Promtail-compatible log export %s (HTTP %d)", e.reason, e.status)
	}
	return "Promtail-compatible log export " + e.reason
}

func (e *exportFailure) Unwrap() error { return e.cause }

type sender struct {
	client   http.Client
	endpoint string
	apiKey   string
	timeout  time.Duration
}

// newSender borrows the discovery client's owned transport, including its proxy and trust.
func newSender(client *http.Client, endpoint, apiKey string, timeout time.Duration) (*sender, error) {
	if client == nil || client.Transport == nil {
		return nil, errors.New("legacy sender requires an explicitly configured HTTP transport")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(endpoint, "#") ||
		!strings.HasSuffix(u.Path, "/stsAgent/logs/k8s") {
		return nil, errors.New("legacy endpoint must be an absolute HTTP(S) URL ending in " +
			"/stsAgent/logs/k8s without userinfo, query or fragment")
	}
	if strings.TrimSpace(apiKey) == "" || strings.ContainsFunc(apiKey, func(r rune) bool { return r < 32 || r == 127 }) {
		return nil, errors.New("legacy sender requires a valid API key")
	}
	if timeout <= 0 {
		return nil, errors.New("legacy attempt timeout must be positive")
	}
	owned := *client
	owned.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &sender{client: owned, endpoint: endpoint, apiKey: apiKey, timeout: timeout}, nil
}

func (s *sender) send(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return contextFailure(err)
	}
	attempt, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(attempt, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		return consumererror.NewPermanent(&exportFailure{reason: failureConfiguration})
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("sts-api-key", s.apiKey)
	response, err := s.client.Do(request)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return contextFailure(ctx.Err())
		}
		if attempt.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return &exportFailure{reason: failureAttemptTimeout}
		}
		var verification *tls.CertificateVerificationError
		var unknown x509.UnknownAuthorityError
		var hostname x509.HostnameError
		var invalid x509.CertificateInvalidError
		if errors.As(err, &verification) || errors.As(err, &unknown) ||
			errors.As(err, &hostname) || errors.As(err, &invalid) {
			return consumererror.NewPermanent(&exportFailure{reason: failureConfiguration})
		}
		return &exportFailure{reason: "transport"}
	}
	status := response.StatusCode
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == 401 || status == 403:
		return consumererror.NewPermanent(&exportFailure{reason: "authentication", status: status})
	case status == 408 || status == 429 || status >= 500 && status < 600:
		failure := &exportFailure{reason: "transient", status: status}
		if status == 429 || status == 503 {
			if delay := retryAfter(response.Header.Get("Retry-After"), time.Now()); delay > 0 {
				return exporterhelper.NewThrottleRetry(failure, delay)
			}
		}
		return failure
	default:
		return consumererror.NewPermanent(&exportFailure{reason: "rejected", status: status})
	}
}

func contextFailure(err error) error {
	reason := "canceled"
	if errors.Is(err, context.DeadlineExceeded) {
		reason = "deadline_expired"
	}
	return &exportFailure{reason: reason, cause: err}
}

func retryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		if seconds > uint64(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(seconds) * time.Second
	} else if errors.Is(err, strconv.ErrRange) && strings.Trim(value, "0123456789") == "" {
		return time.Duration(math.MaxInt64)
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 0
}

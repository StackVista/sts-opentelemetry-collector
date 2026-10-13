package stsk8seventsexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/telemetry"
	"github.com/StackVista/stackstate-receiver-go-client/pkg/transactional"
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

type intakeSender struct {
	client      *http.Client
	endpoint    string
	apiKey      string
	userAgent   string
	maxAttempts int
	backoff     func(attempt int) time.Duration
}

func defaultBackoff(attempt int) time.Duration {
	return min(time.Second<<attempt, 30*time.Second)
}

// buildPayloads groups events by source type, as the cluster agent does, and
// splits them into requests of at most maxEvents.
func buildPayloads(hostname, sourceType string, events []telemetry.Event, maxEvents int) []transactional.IntakePayload {
	var payloads []transactional.IntakePayload
	for start := 0; start < len(events); start += maxEvents {
		end := min(start+maxEvents, len(events))
		// NewIntakePayload sends empty rather than null lists, which the Receiver rejects.
		payload := transactional.NewIntakePayload()
		payload.InternalHostname = hostname
		payload.Events[sourceType] = events[start:end]
		payloads = append(payloads, payload)
	}
	return payloads
}

func encode(payload transactional.IntakePayload) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if err := json.NewEncoder(writer).Encode(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *intakeSender) sendWithRetry(ctx context.Context, payload transactional.IntakePayload) error {
	body, err := encode(payload)
	if err != nil {
		return &permanentError{err}
	}
	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.backoff(attempt - 1)):
			}
		}
		err = s.send(ctx, body)
		var permanent *permanentError
		if err == nil || errors.As(err, &permanent) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (s *intakeSender) send(ctx context.Context, body []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return &permanentError{err}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("sts-api-key", s.apiKey)
	if s.userAgent != "" {
		request.Header.Set("User-Agent", s.userAgent)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("intake transport failure: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))

	status := response.StatusCode
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		return fmt.Errorf("intake returned HTTP %d", status)
	default:
		return &permanentError{fmt.Errorf("intake rejected request with HTTP %d", status)}
	}
}

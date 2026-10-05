package stsk8stopologyexporter

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

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/topology"
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

// buildPayloads splits one snapshot into ordered intake payloads: the first
// starts the snapshot and the last stops it.
func buildPayloads(
	hostname string, instance topology.Instance, components []topology.Component, relations []topology.Relation,
	maxElements int,
) []transactional.IntakePayload {
	var chunks []topology.Topology
	// The Receiver rejects null lists, so every list is sent, empty if need be.
	empty := func() topology.Topology {
		return topology.Topology{
			Instance: instance, Components: []topology.Component{}, Relations: []topology.Relation{}, DeleteIDs: []string{},
		}
	}
	current := empty()
	size := 0
	flush := func() {
		chunks = append(chunks, current)
		current = empty()
		size = 0
	}
	for _, component := range components {
		if size == maxElements {
			flush()
		}
		current.Components = append(current.Components, component)
		size++
	}
	for _, relation := range relations {
		if size == maxElements {
			flush()
		}
		current.Relations = append(current.Relations, relation)
		size++
	}
	flush()
	chunks[0].StartSnapshot = true
	chunks[len(chunks)-1].StopSnapshot = true

	payloads := make([]transactional.IntakePayload, 0, len(chunks))
	for _, chunk := range chunks {
		payload := transactional.NewIntakePayload()
		payload.InternalHostname = hostname
		payload.Topologies = []topology.Topology{chunk}
		payloads = append(payloads, payload)
	}
	return payloads
}

// sendAll posts payloads in order, stopping at the first one that fails.
func (s *intakeSender) sendAll(ctx context.Context, payloads []transactional.IntakePayload) error {
	for i := range payloads {
		body, err := encode(payloads[i])
		if err != nil {
			return err
		}
		if err := s.sendWithRetry(ctx, body); err != nil {
			return fmt.Errorf("intake request %d of %d: %w", i+1, len(payloads), err)
		}
	}
	return nil
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

func (s *intakeSender) sendWithRetry(ctx context.Context, body []byte) error {
	var err error
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

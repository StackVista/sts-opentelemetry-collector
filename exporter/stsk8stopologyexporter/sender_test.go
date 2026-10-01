package stsk8stopologyexporter //nolint:testpackage // Exercises the internal payload splitter and sender.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/StackVista/stackstate-receiver-go-client/pkg/model/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSender(url string) *intakeSender {
	return &intakeSender{
		client:      http.DefaultClient,
		endpoint:    url,
		apiKey:      "k",
		maxAttempts: 3,
		backoff:     func(int) time.Duration { return time.Millisecond },
	}
}

func TestBuildPayloadsBracketsSnapshot(t *testing.T) {
	instance := topology.Instance{Type: "kubernetes", URL: "c"}
	components := []topology.Component{{ExternalID: "a"}, {ExternalID: "b"}, {ExternalID: "c"}}
	relations := []topology.Relation{{ExternalID: "r"}}

	payloads := buildPayloads("h", instance, components, relations, 2)
	require.Len(t, payloads, 2)
	assert.True(t, payloads[0].Topologies[0].StartSnapshot)
	assert.False(t, payloads[0].Topologies[0].StopSnapshot)
	assert.False(t, payloads[1].Topologies[0].StartSnapshot)
	assert.True(t, payloads[1].Topologies[0].StopSnapshot)
	assert.Len(t, payloads[1].Topologies[0].Relations, 1)

	empty := buildPayloads("h", instance, nil, nil, 2)
	require.Len(t, empty, 1, "an empty cluster still sends a snapshot so removed elements are deleted")
	assert.True(t, empty[0].Topologies[0].StartSnapshot)
	assert.True(t, empty[0].Topologies[0].StopSnapshot)
}

func TestSenderRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	payloads := buildPayloads("h", topology.Instance{}, nil, nil, 10)
	require.NoError(t, testSender(server.URL).sendAll(context.Background(), payloads))
	assert.Equal(t, int32(2), calls.Load())
}

func TestSenderStopsOnRejectedRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	components := []topology.Component{{ExternalID: "a"}, {ExternalID: "b"}}
	payloads := buildPayloads("h", topology.Instance{}, components, nil, 1)
	require.Error(t, testSender(server.URL).sendAll(context.Background(), payloads))
	assert.Equal(t, int32(1), calls.Load(), "no retry and no later chunks after a rejection")
}

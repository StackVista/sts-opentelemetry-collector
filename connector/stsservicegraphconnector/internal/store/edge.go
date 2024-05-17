// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package store // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector/internal/store"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type ConnectionType string

const (
	Unknown         ConnectionType = ""
	MessagingSystem ConnectionType = "messaging_system"
	Database        ConnectionType = "database"
)

// Edge is an Edge between two nodes in the graph
type Edge struct {
	Key Key

	TraceID                            pcommon.TraceID
	ConnectionType                     ConnectionType
	ServerService, ClientService       string
	ServerLatencySec, ClientLatencySec float64

	// If either the client or the server spans have status code error,
	// the Edge will be considered as failed.
	Failed bool

	// Additional dimension to add to the metrics
	Dimensions map[string]string

	// expiration is the time at which the Edge expires, expressed as Unix time
	expiration time.Time
	// generation is the number of times the edge has been re-inserted
	generation int
	// logp is the probability this edge was retained after evictions
	logp float64

	PeerService string
}

func newEdge(key Key, ttl time.Duration) *Edge {
	return &Edge{
		Key:        key,
		Dimensions: make(map[string]string),
		expiration: time.Now().Add(ttl),
		generation: 0,
		logp:       0.0,
	}
}

// isComplete returns true if the corresponding client and server
// pair spans have been processed for the given Edge
func (e *Edge) isComplete() bool {
	return len(e.ClientService) != 0 && len(e.ServerService) != 0
}

func (e *Edge) isExpired(time time.Time) bool {
	return time.After(e.expiration)
}

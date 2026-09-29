package logsagent

import (
	"sync"
	"time"
)

// Controller shares delivery accounting and the absolute drain deadline.
type Controller interface {
	RegisterExportObserver(ExportObserver) error
	DrainDeadline() time.Time
	RetryBound() time.Duration
}

// ExportObserver counts synchronous delivery calls, not exporter workers.
type ExportObserver interface {
	Snapshot() ExportSnapshot
}

type ExportSnapshot struct {
	Outstanding     int64
	Acknowledged    int64
	Failed          int64
	DeadlineExpired int64
	DrainRejected   int64
}

type Accounting struct {
	mu    sync.Mutex
	state ExportSnapshot
}

func (a *Accounting) Start() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.Outstanding++
}

func (a *Accounting) Finish(outcome string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.Outstanding--
	if outcome == "acknowledged" {
		a.state.Acknowledged++
	} else {
		a.state.Failed++
		if outcome == "deadline_expired" {
			a.state.DeadlineExpired++
		}
	}
}

func (a *Accounting) RejectDrain() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.DrainRejected++
}

func (a *Accounting) Snapshot() ExportSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

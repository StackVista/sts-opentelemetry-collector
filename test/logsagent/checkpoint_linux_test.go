//nolint:goconst // Keep route names beside the checkpoint failure scenario.
package logsagent_test

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCheckpointWriteFailureReplaysAfterRestart(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	before := f.appendRecords(0, 0, 3)
	p := f.start(nil, true)
	p.ready()
	f.backend.waitBodies(before, true)
	p.signal()
	p.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p, true)

	p2 := f.start(stressValidated(t, nil), true)
	p2.ready()
	var limit unix.Rlimit
	if err := unix.Prlimit(p2.cmd.Process.Pid, unix.RLIMIT_FSIZE, nil, &limit); err != nil {
		t.Fatal(err)
	}
	// Fail writes even to an already-open checkpoint file, including when tests run as root.
	limit.Cur = 0
	if err := unix.Prlimit(p2.cmd.Process.Pid, unix.RLIMIT_FSIZE, &limit, nil); err != nil {
		t.Fatal(err)
	}
	after := f.appendRecords(0, 3, 2)
	f.backend.waitBodies(after, true)
	eventually(t, time.Second, "upstream checkpoint-save error", func() bool {
		return p2.event("save offsets", nil)
	})
	p2.ready()
	p2.signal()
	p2.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p2, true)
	f.backend.assertRecords(append(before, after...), true)

	p3 := f.start(nil, true)
	p3.ready()
	eventually(t, 5*time.Second, "replay after failed checkpoint writes", func() bool {
		counts := f.backend.bodies(true)
		for _, body := range after {
			if counts[body] != 2 {
				return false
			}
		}
		return true
	})
	p3.signal()
	p3.wait(5*time.Second, true)
	assertOrdinaryDrain(t, p3, true)
	assertBodyCopies(t, f.backend, before, 1)
	assertBodyCopies(t, f.backend, after, 2)
	f.backend.assertOnly()
	f.backend.assertIdentity()
}

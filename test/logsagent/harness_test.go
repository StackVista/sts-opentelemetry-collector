//nolint:goconst // Keep fixture keys and expected event fields visible at their assertions.
package logsagent_test

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"text/template"
	"time"

	"go.yaml.in/yaml/v3"
)

const syntheticKey = "synthetic-logsagent-fixture"

//go:embed testdata/agent.yaml.tmpl
var configTemplate string

func TestMain(m *testing.M) {
	requireAgent := flag.Bool("require-agent", false, "fail instead of skipping when OTEL_AGENT_BINARY is absent")
	requireFaultAgent := flag.Bool("require-fault-agent", false, "fail when OTEL_AGENT_FAULT_BINARY is absent")
	flag.Parse()
	if *requireAgent && os.Getenv("OTEL_AGENT_BINARY") == "" {
		fmt.Fprintln(os.Stderr, "-require-agent needs OTEL_AGENT_BINARY")
		os.Exit(2)
	}
	if *requireFaultAgent && os.Getenv("OTEL_AGENT_FAULT_BINARY") == "" {
		fmt.Fprintln(os.Stderr, "-require-fault-agent needs OTEL_AGENT_FAULT_BINARY")
		os.Exit(2)
	}
	os.Exit(m.Run())
}

type settings struct {
	Checkpoints, Health                   string
	Include, PromtailURL                  string
	Files, Concurrency                    int
	Lifetime, RetryBudget, AttemptTimeout time.Duration
}

func defaultSettings() settings {
	return settings{
		Files: 4, Concurrency: 8,
		Lifetime: 25 * time.Second, RetryBudget: 2 * time.Second, AttemptTimeout: 200 * time.Millisecond,
	}
}

func (s settings) minimumLifetime() time.Duration {
	return (s.RetryBudget + s.AttemptTimeout) + 20*time.Second
}

type fixture struct {
	t         *testing.T
	binary    string
	root      string
	settings  settings
	backend   *backend
	childEnv  []string
	configure func(map[string]any)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newBinaryFixture(t, "OTEL_AGENT_BINARY")
}

func newBinaryFixture(t *testing.T, variable string) *fixture {
	t.Helper()
	binary := os.Getenv(variable)
	if binary == "" {
		t.Skipf("%s absent; assembled-process test not run", variable)
	}
	absolute, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(absolute)
	if err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		t.Fatalf("%s must name an executable file: %s", variable, absolute)
	}
	root := t.TempDir()
	b := newBackend(t)
	s := defaultSettings()
	s.Checkpoints = filepath.Join(root, "checkpoints")
	s.Include = filepath.Join(root, "pods", "*", "*", "*.log")
	s.PromtailURL = b.server.URL + "/stsAgent/logs/k8s"
	return &fixture{t: t, binary: absolute, root: root, settings: s, backend: b}
}

func renderConfig(t *testing.T, s settings) map[string]any {
	t.Helper()
	tmpl, err := template.New("agent").Parse(configTemplate)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, s); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func childEnvironment(root string) []string {
	return []string{
		"HOME=" + root,
		"TMPDIR=" + root,
		"PATH=/usr/bin:/bin",
		"GOMAXPROCS=2",
		"STS_API_KEY=" + syntheticKey,
	}
}

func (f *fixture) start(mutate func(map[string]any), synchronous bool) *process {
	f.t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.t.Fatal(err)
	}
	f.settings.Health = listener.Addr().String()
	if err := listener.Close(); err != nil {
		f.t.Fatal(err)
	}
	config := renderConfig(f.t, f.settings)
	if f.configure != nil {
		f.configure(config)
	}
	if mutate != nil {
		mutate(config)
	}
	data, err := yaml.Marshal(config)
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(f.root, "agent.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	gate := "-stanza.synchronousLogEmitter"
	if synchronous {
		gate = "stanza.synchronousLogEmitter"
	}
	cmd := exec.Command(f.binary, "--feature-gates="+gate, "--config="+path) //nolint:gosec // Runs the explicitly supplied test binary with synthetic configuration.
	cmd.Dir = f.root
	cmd.Env = append(childEnvironment(f.root), f.childEnv...)
	p := &process{
		t: f.t, cmd: cmd, done: make(chan struct{}), health: "http://" + f.settings.Health,
		client: &http.Client{
			Timeout:   200 * time.Millisecond,
			Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		},
	}
	cmd.Stdout, cmd.Stderr = &p.output, &p.output
	if err := cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	f.t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = cmd.Process.Kill()
			<-p.done
		}
		if f.t.Failed() {
			f.t.Logf("agent output:\n%s", p.output.String())
		}
	})
	return p
}

func (f *fixture) appendRecords(file, first, count int) []string {
	f.t.Helper()
	path := f.sourcePath(file)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		f.t.Fatal(err)
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		f.t.Fatal(err)
	}
	var bodies []string
	for i := first; i < first+count; i++ {
		body := fmt.Sprintf("file=%d record=%d %s", file, i, strings.Repeat("synthetic ", 16))
		// Keep the initial fingerprint stable when appending to a short file.
		if _, err := fmt.Fprintf(out, "2026-09-10T12:00:00.123456789Z stdout F %s\n", body); err != nil {
			_ = out.Close()
			f.t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	if err := out.Close(); err != nil {
		f.t.Fatal(err)
	}
	return bodies
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type process struct {
	t      *testing.T
	cmd    *exec.Cmd
	output lockedBuffer
	done   chan struct{}
	err    error
	health string
	client *http.Client
}

func (p *process) status(path string) int {
	resp, err := p.client.Get(p.health + path)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-tick.C:
		}
	}
}

func (p *process) ready() {
	p.t.Helper()
	eventually(p.t, 10*time.Second, "agent readiness", func() bool {
		select {
		case <-p.done:
			p.t.Fatalf("agent exited before readiness: %v", p.err)
		default:
		}
		return p.status("/ready") == http.StatusOK
	})
}

func (p *process) signal() {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatal(err)
	}
}

func (p *process) wait(timeout time.Duration, success bool) {
	p.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		if (p.err == nil) != success {
			p.t.Fatalf("process exit = %v, want success=%v", p.err, success)
		}
	case <-timer.C:
		p.t.Fatalf("agent did not exit within %s", timeout)
	}
}

func (p *process) event(message string, fields map[string]any) bool {
	for _, line := range strings.Split(p.output.String(), "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil || event["msg"] != message {
			continue
		}
		match := true
		for key, value := range fields {
			if event[key] != value {
				match = false
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (p *process) assertDrain(outcome string) {
	p.t.Helper()
	if !p.event("Logs export drain finished", map[string]any{"outcome": outcome}) {
		p.t.Fatalf("missing final drain outcome %q", outcome)
	}
}

func section(config map[string]any, path ...string) map[string]any {
	for _, key := range path {
		next, ok := config[key].(map[string]any)
		if !ok {
			panic(fmt.Sprintf("fixture section %q must be a mapping", key))
		}
		config = next
	}
	return config
}

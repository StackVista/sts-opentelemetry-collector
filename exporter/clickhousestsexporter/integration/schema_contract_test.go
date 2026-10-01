//go:build integration && schema_contract

package clickhousestsexporter_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stackstateSchemaRefFallback is only used when STACKSTATE_SCHEMA_REF_VERSION isn't set (e.g. running
// this test outside CI). CI sets that env var from the annotated value in build.yaml, which is what
// Renovate bumps — keep this fallback in sync with it manually until that's automated.
const (
	stackstateSchemaRefFallback = "be45a6d9767c276200cf6f27e41a737366a0ab7c"
	stackstateSchemaRepo        = "StackVista/stackstate"
	stackstateSchemaPath        = "stackstate-traces/src/test/resources/otel_logs_schema.sql"

	fetchAttempts = 3
	fetchBackoff  = 5 * time.Second
)

func stackstateSchemaRef() string {
	if ref := os.Getenv("STACKSTATE_SCHEMA_REF_VERSION"); ref != "" {
		return ref
	}
	return stackstateSchemaRefFallback
}

// fetchStackStateSchema returns the pinned otel_logs/otel_logs_resources DDL as StackState actually
// renders it, so the test below proves this exporter's insert matches the real deployed schema, not a
// hand-maintained copy of it. STACKSTATE_SCHEMA_FILE overrides the fetch with a local file, for
// iterating on a stackstate-side schema change before it's pushed.
//
// Retries a few times with a fixed backoff: a fetch failure only means the schema couldn't be reached
// (rate limit, transient network blip), not that the exporter is broken, and this test's whole point is
// to distinguish that from a real compatibility failure. Retrying here keeps that distinction meaningful
// instead of pushing "was this a flake?" onto whoever reads a red CI check.
func fetchStackStateSchema(t *testing.T) string {
	t.Helper()
	if local := os.Getenv("STACKSTATE_SCHEMA_FILE"); local != "" {
		content, err := os.ReadFile(local)
		require.NoError(t, err)
		return string(content)
	}

	ref := stackstateSchemaRef()
	path := "repos/" + stackstateSchemaRepo + "/contents/" + stackstateSchemaPath + "?ref=" + ref
	var out []byte
	var lastErr error
	for attempt := 1; attempt <= fetchAttempts; attempt++ {
		cmd := exec.CommandContext(t.Context(), "gh", "api", path)
		out, lastErr = cmd.Output()
		if lastErr == nil {
			break
		}
		if attempt < fetchAttempts {
			t.Logf("fetch attempt %d/%d failed, retrying: %v", attempt, fetchAttempts, lastErr)
			time.Sleep(fetchBackoff)
		}
	}
	require.NoError(t, lastErr, fmt.Sprintf("fetching pinned stackstate schema (ref=%s) after %d attempts — this "+
		"failing does not mean the exporter is broken, it means the schema couldn't be fetched; check ref "+
		"validity and gh auth before treating this as a compatibility failure", ref, fetchAttempts))

	var content struct {
		Content string `json:"content"`
	}
	require.NoError(t, json.Unmarshal(out, &content))
	decoded, err := base64.StdEncoding.DecodeString(content.Content)
	require.NoError(t, err)
	return string(decoded)
}

// TestLogsExporter_MatchesStackStateSchema is the schema-drift regression the collector's own DDL copy
// cannot provide: it builds the test table from StackState's real, pinned DDL instead of this exporter's
// own copy, so a divergence between the two shows up here instead of only in a branch deploy.
func TestLogsExporter_MatchesStackStateSchema(t *testing.T) {
	ch := startClickHouse(t)
	schemaSQL := fetchStackStateSchema(t)
	cfg := ch.exporterConfig()
	ctx := context.Background()

	// ch.db has no database selected; the schema (and the exporter, per cfg.Database) needs the
	// per-test database to actually exist and be the active one for CREATE TABLE.
	defaultDB, err := cfg.BuildDB("default")
	require.NoError(t, err)
	defer func() { _ = defaultDB.Close() }()
	_, err = defaultDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+ch.database)
	require.NoError(t, err)

	testDB, err := cfg.BuildDB(ch.database)
	require.NoError(t, err)
	defer func() { _ = testDB.Close() }()

	// One CREATE TABLE per statement; the ClickHouse driver's Exec does not run multiple statements at once.
	for _, statement := range strings.Split(schemaSQL, ";") {
		if trimmed := strings.TrimSpace(statement); trimmed != "" {
			_, err := testDB.ExecContext(ctx, trimmed)
			require.NoError(t, err, "the pinned StackState schema failed to apply — inspect it before assuming the exporter is wrong")
		}
	}

	cfg.CreateLogsTable = false
	exporter := startLogsExporter(t, cfg)
	require.NoError(t, exporter.PushLogsData(ctx, testLogs()))

	rows := ch.logs(t)
	require.Len(t, rows, 2, "the event record is not exported")
	resources := ch.resources(t)
	require.Len(t, resources, 1)
}

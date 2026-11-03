//go:build integration && schema_contract

package clickhousestsexporter_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stackstateSchemaRefFallback is only used when STACKSTATE_SCHEMA_REF_VERSION isn't set (e.g. running
// this test outside CI). CI sets that env var from the annotated value in build.yaml, which is what
// Renovate bumps — keep this fallback in sync with it manually until that's automated.
const (
	stackstateSchemaRefFallback = "ad611298332192416c99ad3ebb33cb9d2145e3da"
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

// STACKSTATE_SCHEMA_FILE permits local validation before the StackState commit is published.
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

func TestLogsExporter_MatchesStackStateSchema(t *testing.T) {
	ch := startClickHouse(t)
	schema := fetchStackStateSchema(t)
	require.Equal(t, schema, localLogSchema(t), "refresh testdata/otel_logs_schema.sql from the pinned StackState commit")
	ch.applyLogSchema(t, schema)
	exporter := startLogsExporter(t, ch.exporterConfig())
	require.NoError(t, exporter.PushLogsData(context.Background(), testLogs()))
	require.Len(t, ch.logs(t), 3)
	require.Len(t, ch.resources(t), 1)
	require.Equal(t, uint64(2), ch.count(t, "SELECT count() FROM %s.otel_logs_trace_lookup_v1"))
}

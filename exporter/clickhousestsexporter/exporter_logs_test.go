// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const testServiceNameAttr = "service.name"

func TestLogsExporter_New(t *testing.T) {
	type validate func(*testing.T, *clickhousestsexporter.LogsExporter, error)

	_ = func(t *testing.T, exporter *clickhousestsexporter.LogsExporter, err error) {
		require.NoError(t, err)
		require.NotNil(t, exporter)
	}

	_ = func(want error) validate {
		return func(t *testing.T, exporter *clickhousestsexporter.LogsExporter, err error) {
			require.Nil(t, exporter)
			require.Error(t, err)
			if !errors.Is(err, want) {
				t.Fatalf("Expected error '%v', but got '%v'", want, err)
			}
		}
	}

	failWithMsg := func(msg string) validate {
		return func(t *testing.T, _ *clickhousestsexporter.LogsExporter, err error) {
			require.Error(t, err)
			require.Contains(t, err.Error(), msg)
		}
	}

	tests := map[string]struct {
		config *clickhousestsexporter.Config
		want   validate
	}{
		"no dsn": {
			config: withDefaultConfig(),
			want:   failWithMsg("parse dsn address failed"),
		},
		"no dsn fails at construction even without table creation": {
			config: withDefaultConfig(func(cfg *clickhousestsexporter.Config) { cfg.CreateLogsTable = false }),
			want: func(t *testing.T, exporter *clickhousestsexporter.LogsExporter, err error) {
				require.Nil(t, exporter)
				require.ErrorContains(t, err, "parse dsn address failed")
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			var err error
			exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), test.config)
			err = errors.Join(err, err)

			if exporter != nil {
				err = errors.Join(err, exporter.Start(context.TODO(), nil))
				defer func() {
					require.NoError(t, exporter.Shutdown(context.TODO()))
				}()
			}

			test.want(t, exporter, err)
		})
	}
}

const (
	logsTable          = "otel_logs"
	logsResourcesTable = "otel_logs_resources"
)

type logsInserts struct {
	logs      [][]driver.Value
	resources [][]driver.Value
	creates   []string
}

func recordLogsInserts(t *testing.T) *logsInserts {
	inserts := &logsInserts{}
	initClickhouseTestServer(t, func(query string, values []driver.Value) error {
		switch {
		case strings.HasPrefix(strings.TrimSpace(query), "CREATE"):
			inserts.creates = append(inserts.creates, query)
		case strings.HasPrefix(query, "INSERT INTO "+logsResourcesTable+" "):
			inserts.resources = append(inserts.resources, values)
		case strings.HasPrefix(query, "INSERT INTO "+logsTable+" "):
			inserts.logs = append(inserts.logs, values)
		}
		return nil
	})
	return inserts
}

// Inserts use the native batch API, which the fake database/sql driver cannot observe; the
// row mapping is covered by internal tests and the inserts by the integration tests.
func TestLogsExporter_TableCreation(t *testing.T) {
	t.Run("tables are created by default", func(t *testing.T) {
		inserts := recordLogsInserts(t)
		newTestLogsExporter(t)

		require.Len(t, inserts.creates, 2)
		require.Contains(t, inserts.creates[0], logsResourcesTable)
		require.Contains(t, inserts.creates[1], logsTable)
	})
	t.Run("tables are not created when disabled", func(t *testing.T) {
		inserts := recordLogsInserts(t)
		newTestLogsExporter(t, func(cfg *clickhousestsexporter.Config) { cfg.CreateLogsTable = false })

		require.Empty(t, inserts.creates)
	})
	t.Run("resources table TTL has slack beyond the logs TTL", func(t *testing.T) {
		inserts := recordLogsInserts(t)
		newTestLogsExporter(t, func(cfg *clickhousestsexporter.Config) { cfg.TTL = 72 * time.Hour })

		require.Len(t, inserts.creates, 2)
		require.Contains(t, inserts.creates[0], "toIntervalDay(4)")
		require.Contains(t, inserts.creates[1], "toIntervalDay(3)")
	})
}

func newTestLogsExporter(t *testing.T, fns ...func(*clickhousestsexporter.Config)) {
	exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), withTestExporterConfig(t, fns...)(defaultEndpoint))
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.Shutdown(context.TODO()) })
}

func withTestExporterConfig(t *testing.T, fns ...func(*clickhousestsexporter.Config)) func(string) *clickhousestsexporter.Config {
	return func(endpoint string) *clickhousestsexporter.Config {
		configMods := make([]func(*clickhousestsexporter.Config), 0, 1+len(fns))
		configMods = append(configMods, func(cfg *clickhousestsexporter.Config) {
			cfg.Endpoint = endpoint
			cfg.SetDriverName(t.Name())
		})
		configMods = append(configMods, fns...)
		return withDefaultConfig(configMods...)
	}
}

func initClickhouseTestServer(t *testing.T, recorder recorder) {
	sql.Register(t.Name(), &testClickhouseDriver{
		recorder: recorder,
	})
}

type recorder func(query string, values []driver.Value) error

type testClickhouseDriver struct {
	recorder recorder
}

func (t *testClickhouseDriver) Open(_ string) (driver.Conn, error) {
	return &testClickhouseDriverConn{
		recorder: t.recorder,
	}, nil
}

type testClickhouseDriverConn struct {
	recorder recorder
}

func (t *testClickhouseDriverConn) Prepare(query string) (driver.Stmt, error) {
	return &testClickhouseDriverStmt{
		query:    query,
		recorder: t.recorder,
	}, nil
}

func (*testClickhouseDriverConn) Close() error {
	return nil
}

func (*testClickhouseDriverConn) Begin() (driver.Tx, error) {
	return &testClickhouseDriverTx{}, nil
}

func (*testClickhouseDriverConn) CheckNamedValue(_ *driver.NamedValue) error {
	return nil
}

type testClickhouseDriverStmt struct {
	query    string
	recorder recorder
}

func (*testClickhouseDriverStmt) Close() error {
	return nil
}

func (t *testClickhouseDriverStmt) NumInput() int {
	return strings.Count(t.query, "?")
}

func (t *testClickhouseDriverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, t.recorder(t.query, args)
}

func (t *testClickhouseDriverStmt) Query(_ []driver.Value) (driver.Rows, error) {
	//nolint:nilnil
	return nil, nil
}

type testClickhouseDriverTx struct {
}

func (*testClickhouseDriverTx) Commit() error {
	return nil
}

func (*testClickhouseDriverTx) Rollback() error {
	return nil
}

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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
			want:   failWithMsg("exec create resources table sql: parse dsn address failed"),
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

func TestExporter_pushLogsData(t *testing.T) {
	t.Run("push success", func(t *testing.T) {
		var logItems int
		var resourceItems int
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			t.Logf("values:%+v", values)
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_logs") {
				logItems++
			}
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_resources") {
				resourceItems++
			}
			return nil
		})

		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
		mustPushLogsData(t, exporter, simpleLogs(2))

		require.Equal(t, 3, logItems)
		require.Equal(t, 2, resourceItems)
	})
	t.Run("test check resource metadata", func(t *testing.T) {
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_logs") {
				require.Equal(t, "https://opentelemetry.io/schemas/1.4.0", values[16])
				require.NotEmpty(t, values[17])
				require.Equal(t, "test-service", values[23])
				require.Equal(t, []string{"k8s.cluster.name:test-cluster", "k8s.scope:test-cluster/test-namespace"}, values[26])
			}
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_resources") {
				require.Equal(t, map[string]string{
					testServiceNameAttr:   "test-service",
					"k8s.cluster.name":    "test-cluster",
					"k8s.namespace.name":  "test-namespace",
					"service.namespace":   "test-service-namespace",
					"service.instance.id": "test-instance",
				}, values[2])
				require.Equal(t, []string{"k8s.cluster.name:test-cluster", "k8s.scope:test-cluster/test-namespace"}, values[3])
			}
			return nil
		})
		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
	})
	t.Run("test check scope metadata", func(t *testing.T) {
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_logs") {
				require.Equal(t, "https://opentelemetry.io/schemas/1.7.0", values[18])
				require.Equal(t, "io.opentelemetry.contrib.clickhouse", values[19])
				require.Equal(t, "1.0.0", values[20])
				require.Equal(t, map[string]string{
					"lib": "clickhouse",
				}, values[21])
				require.JSONEq(t, `{"lib":"clickhouse"}`, values[22].(string))
			}
			return nil
		})
		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
	})
	t.Run("test check log metadata", func(t *testing.T) {
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_logs") {
				require.Equal(t, "timestamp", values[2])
				require.NotEmpty(t, values[3])
				require.Equal(t, "otlp-collector", values[4])
				require.Equal(t, values[3], values[5])
				require.Equal(t, "", values[6])
				require.Equal(t, uint8(0), values[9])
				require.Equal(t, "INFO", values[10])
				require.Equal(t, uint8(9), values[11])
				require.Equal(t, "otel log body", values[12])
				require.Equal(t, "string", values[13])
				require.Equal(t, "test.event", values[15])
				require.Equal(t, map[string]string{
					"event.name": "test.event",
					"log.kind":   "smoke",
				}, values[24])
				require.JSONEq(t, `{"event.name":"test.event","log.kind":"smoke"}`, values[25].(string))
			}
			return nil
		})
		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
	})
}

func newTestLogsExporter(t *testing.T, dsn string, fns ...func(*clickhousestsexporter.Config)) *clickhousestsexporter.LogsExporter {
	exporter, err := clickhousestsexporter.NewLogsExporter(zaptest.NewLogger(t), withTestExporterConfig(t, fns...)(dsn))
	require.NoError(t, err)
	require.NoError(t, exporter.Start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.Shutdown(context.TODO()) })
	return exporter
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

func simpleLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rl.Resource().Attributes().PutStr(testServiceNameAttr, "test-service")
	rl.Resource().Attributes().PutStr("service.namespace", "test-service-namespace")
	rl.Resource().Attributes().PutStr("service.instance.id", "test-instance")
	rl.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")
	rl.Resource().Attributes().PutStr("k8s.namespace.name", "test-namespace")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("io.opentelemetry.contrib.clickhouse")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "clickhouse")
	for i := 0; i < count; i++ {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.SetSeverityText("INFO")
		r.SetSeverityNumber(plog.SeverityNumberInfo)
		r.Body().SetStr("otel log body")
		r.Attributes().PutStr("event.name", "test.event")
		r.Attributes().PutStr("log.kind", "smoke")
	}
	return logs
}

func mustPushLogsData(t *testing.T, exporter *clickhousestsexporter.LogsExporter, ld plog.Logs) {
	err := exporter.PushLogsData(context.TODO(), ld)
	require.NoError(t, err)
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

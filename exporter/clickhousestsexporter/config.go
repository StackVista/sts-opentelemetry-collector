// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//nolint:lll
package clickhousestsexporter // import "github.com/stackvista/sts-opentelemetry-collector/exporter/clickhousestsexporter"

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for Elastic exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`

	// Endpoint is the clickhouse endpoint.
	Endpoint string `mapstructure:"endpoint"`
	// Username is the authentication username.
	Username string `mapstructure:"username"`
	// Username is the authentication password.
	Password configopaque.String `mapstructure:"password"`
	// Database is the database name to export.
	Database string `mapstructure:"database"`
	// ConnectionParams is the extra connection parameters with map format. for example compression/dial_timeout
	ConnectionParams map[string]string `mapstructure:"connection_params"`
	// LogsTableName is the table name for logs. default is `otel_logs`.
	LogsTableName string `mapstructure:"logs_table_name"`
	// TracesTableName is the table name for logs. default is `otel_traces`.
	TracesTableName string `mapstructure:"traces_table_name"`
	// MetricsTableName is the table name for metrics. default is `otel_metrics`.
	MetricsTableName string `mapstructure:"metrics_table_name"`
	// ResourcesTableName is the table name for resources. default is `otel_resources`.
	ResourcesTableName string `mapstructure:"resources_table_name"`
	// ComponentsTableName is the table name for components. default is `otel_components`.
	ComponentsTableName string `mapstructure:"components_table_name"`
	// RelationsTableName is the table name for relations. default is `otel_relations`.
	RelationsTableName string `mapstructure:"relations_table_name"`
	// ComponentsTimeRangeTableName is the table name for components time range. default is `otel_components_time_range`.
	ComponentsTimeRangeTableName string `mapstructure:"components_time_range_table_name"`
	// RelationsTimeRangeTableName is the table name for relations time range. default is `otel_relations_time_range`.
	RelationsTimeRangeTableName string `mapstructure:"relations_time_range_table_name"`
	// ComponentsFieldValuesTableName is the table name for components field values. default is `otel_components_field_values`.
	ComponentsFieldValuesTableName string `mapstructure:"components_field_values_table_name"`
	// RelationsFieldValuesTableName is the table name for relations field values. default is `otel_relations_field_values`.
	RelationsFieldValuesTableName string `mapstructure:"relations_field_values_table_name"`
	// ComponentsTimeRangeMVName is the materialized view name for components time range. default is `otel_components_time_range_mv`.
	ComponentsTimeRangeMVName string `mapstructure:"components_time_range_mv_name"`
	// RelationsTimeRangeMVName is the materialized view name for relations time range. default is `otel_relations_time_range_mv`.
	RelationsTimeRangeMVName string `mapstructure:"relations_time_range_mv_name"`
	// ComponentsFieldValuesMVName is the materialized view name for components field values. default is `otel_components_field_values_mv`.
	ComponentsFieldValuesMVName string `mapstructure:"components_field_values_mv_name"`
	// RelationsFieldValuesMVName is the materialized view name for relations field values. default is `otel_relations_field_values_mv`.
	RelationsFieldValuesMVName string `mapstructure:"relations_relations_field_values_mv_name"`

	// Deprecated: Use 'ttl' instead
	TTLDays uint `mapstructure:"ttl_days"`
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`
	// Create the traces table on startup
	CreateTracesTable bool `mapstructure:"create_traces_table"`
	// Create the logs table on startup
	CreateLogsTable bool `mapstructure:"create_logs_table"`
	// Create the metrics table on startup
	CreateMetricsTable bool `mapstructure:"create_metrics_table"`
	// Create the resources table on startup
	CreateResourcesTable bool `mapstructure:"create_resources_table"`
	// Create the topology table on startup
	CreateTopologyTable bool `mapstructure:"create_topology_table"`
	// EnableLogs enables the logs exporter.
	EnableLogs bool `mapstructure:"enable_logs"`
	// EnableTopology enables the topology exporter.
	EnableTopology bool `mapstructure:"enable_topology"`

	driverName string
}

const defaultDatabase = "default"

var (
	ErrConfigNoEndpoint      = errors.New("endpoint must be specified")
	ErrConfigInvalidEndpoint = errors.New("endpoint must be url format")
	//nolint:lll
	ErrConfigTTL = errors.New("both 'ttl_days' and 'ttl' can not be provided. 'ttl_days' is deprecated, use 'ttl' instead")
)

// Validate the clickhouse server configuration.
func (cfg *Config) Validate() error {
	var err error
	if cfg.Endpoint == "" {
		err = errors.Join(err, ErrConfigNoEndpoint)
	}
	dsn, e := cfg.BuildDSN(cfg.Database)
	if e != nil {
		err = errors.Join(err, e)
	}

	if cfg.TTL > 0 && cfg.TTLDays > 0 {
		err = errors.Join(err, ErrConfigTTL)
	}

	// Validate DSN with clickhouse driver.
	// Last chance to catch invalid config.
	if _, e := clickhouse.ParseDSN(dsn); e != nil {
		err = errors.Join(err, e)
	}

	return err
}

func (cfg *Config) BuildDSN(database string) (string, error) {
	dsnURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrConfigInvalidEndpoint, err.Error())
	}

	queryParams := dsnURL.Query()

	// Add connection params to query params.
	for k, v := range cfg.ConnectionParams {
		queryParams.Set(k, v)
	}

	// Enable TLS if scheme is https. This flag is necessary to support https connections.
	if dsnURL.Scheme == "https" {
		queryParams.Set("secure", "true")
	}

	// Override database if specified in config.
	if cfg.Database != "" {
		dsnURL.Path = cfg.Database
	}

	// Override database if specified in database param.
	if database != "" {
		dsnURL.Path = database
	}

	// Use default database if not specified in any other place.
	if database == "" && cfg.Database == "" && dsnURL.Path == "" {
		dsnURL.Path = defaultDatabase
	}

	// Override username and password if specified in config.
	if cfg.Username != "" {
		dsnURL.User = url.UserPassword(cfg.Username, string(cfg.Password))
	}

	dsnURL.RawQuery = queryParams.Encode()

	return dsnURL.String(), nil
}

func (cfg *Config) BuildDB(database string) (*sql.DB, error) {
	dsn, err := cfg.BuildDSN(database)
	if err != nil {
		return nil, err
	}

	// Safeguard against zero-values
	if cfg.driverName == "" {
		cfg.driverName = "clickhouse"
	}

	// ClickHouse sql driver will read clickhouse settings from the DSN string.
	// It also ensures defaults.
	// See https://github.com/ClickHouse/clickhouse-go/blob/08b27884b899f587eb5c509769cd2bdf74a9e2a1/clickhouse_std.go#L189
	conn, err := sql.Open(cfg.driverName, dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil

}

// utility testing function
func (cfg *Config) SetDriverName(driverName string) {
	cfg.driverName = driverName
}

func (cfg *Config) GetTTLDays() uint {
	return cfg.TTLDays
}

func (cfg *Config) GetTTL() time.Duration {
	return cfg.TTL
}

func (cfg *Config) GetComponentsTableName() string {
	return cfg.ComponentsTableName
}

func (cfg *Config) GetRelationsTableName() string {
	return cfg.RelationsTableName
}

func (cfg *Config) GetComponentsTimeRangeTableName() string {
	return cfg.ComponentsTimeRangeTableName
}

func (cfg *Config) GetRelationsTimeRangeTableName() string {
	return cfg.RelationsTimeRangeTableName
}

func (cfg *Config) GetComponentsFieldValuesTableName() string {
	return cfg.ComponentsFieldValuesTableName
}

func (cfg *Config) GetRelationsFieldValuesTableName() string {
	return cfg.RelationsFieldValuesTableName
}

func (cfg *Config) GetComponentsTimeRangeMVName() string {
	return cfg.ComponentsTimeRangeMVName
}

func (cfg *Config) GetRelationsTimeRangeMVName() string {
	return cfg.RelationsTimeRangeMVName
}

func (cfg *Config) GetComponentsFieldValuesMVName() string {
	return cfg.ComponentsFieldValuesMVName
}

func (cfg *Config) GetRelationsFieldValuesMVName() string {
	return cfg.RelationsFieldValuesMVName
}

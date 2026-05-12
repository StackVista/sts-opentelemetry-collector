package stskafkaexporter_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter"
	"github.com/stretchr/testify/assert"
)

const (
	testBroker       = "localhost:9092"
	testTopic        = "test-topic"
	requiredAcksNone = "none"
)

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *stskafkaexporter.Config
		wantErr error
	}{
		{
			name: "valid config",
			cfg: &stskafkaexporter.Config{
				Brokers:        []string{testBroker},
				Topic:          testTopic,
				ReadTimeout:    2 * time.Second,
				ProduceTimeout: 5 * time.Second,
				RequiredAcks:   requiredAcksNone,
			},
			wantErr: nil,
		},
		{
			name: "missing brokers",
			cfg: &stskafkaexporter.Config{
				Topic:          testTopic,
				ReadTimeout:    2 * time.Second,
				ProduceTimeout: 5 * time.Second,
				RequiredAcks:   requiredAcksNone,
			},
			wantErr: errors.New("at least one kafka broker must be specified"),
		},
		{
			name: "missing topic",
			cfg: &stskafkaexporter.Config{
				Brokers:        []string{testBroker},
				ReadTimeout:    2 * time.Second,
				ProduceTimeout: 5 * time.Second,
				RequiredAcks:   requiredAcksNone,
			},
			wantErr: errors.New("'topic' must be specified"),
		},
		{
			name: "missing read timeout",
			cfg: &stskafkaexporter.Config{
				Brokers:        []string{testBroker},
				Topic:          testTopic,
				ProduceTimeout: 5 * time.Second,
				RequiredAcks:   requiredAcksNone,
			},
			wantErr: errors.New("'read_timeout' must be greater than 0"),
		},
		{
			name: "missing produce timeout",
			cfg: &stskafkaexporter.Config{
				Brokers:      []string{testBroker},
				Topic:        testTopic,
				ReadTimeout:  2 * time.Second,
				RequiredAcks: requiredAcksNone,
			},
			wantErr: errors.New("'produce_timeout' must be greater than 0"),
		},
		{
			name: "missing required acks",
			cfg: &stskafkaexporter.Config{
				Brokers:        []string{testBroker},
				Topic:          testTopic,
				ReadTimeout:    2 * time.Second,
				ProduceTimeout: 5 * time.Second,
			},
			wantErr: errors.New("invalid 'required_acks' value: '' (must be one of: none, leader, all)"),
		},
		{
			name: "invalid required acks",
			cfg: &stskafkaexporter.Config{
				Brokers:        []string{testBroker},
				Topic:          testTopic,
				ReadTimeout:    2 * time.Second,
				ProduceTimeout: 5 * time.Second,
				RequiredAcks:   "foo",
			},
			wantErr: errors.New("invalid 'required_acks' value: 'foo' (must be one of: none, leader, all)"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

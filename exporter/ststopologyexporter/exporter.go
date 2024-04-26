// Copyright StackState B.V.
// SPDX-License-Identifier: Apache-2.0
package ststopologyexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/exporter/ststopologyexporter/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

var (
	errInternal = errors.New("internal error")
)

type topologyExporter struct {
	logger     *zap.Logger
	httpClient http.Client
	cfg        *Config
}

func newTopologyExporter(logger *zap.Logger, cfg component.Config) (*topologyExporter, error) {
	stsCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config passed to stackstateexporter: %T", cfg)
	}
	httpClient := http.Client{
		Timeout: 5 * time.Second, // TODO configure timeout
	}

	return &topologyExporter{logger: logger, httpClient: httpClient, cfg: stsCfg}, nil
}
func (t *topologyExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	log := t.logger

	componentsByApiKey := make(map[string]*internal.ComponentsCollection, 0)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		resource := rs.Resource()
		sts_api_key_value, key_exists := resource.Attributes().Get("sts_api_key")
		if !key_exists {
			log.Warn("No sts_api_key attribute found on resource")
			continue
		}
		sts_api_key := sts_api_key_value.AsString()
		attrs := resource.Attributes()
		attrs.Remove("sts_api_key")
		collection, has_siblings := componentsByApiKey[sts_api_key]
		if !has_siblings {
			collection = internal.NewCollection()
			componentsByApiKey[sts_api_key] = collection
		}
		if !collection.AddResource(&attrs) {
			log.Warn("Skipping resource without necessary attributes")
		}
	}

	for apiKey, collection := range componentsByApiKey {
		components := collection.GetComponents()
		relations := collection.GetRelations()
		request := internal.IntakeTopology{
			CollectionTimestamp: time.Now().UnixMilli(),
			InternalHostname:    "sts-otel-collector",
			Topologies: []internal.Topology{{
				Instance: internal.Instance{
					Type: "opentelemetry",
					URL:  "collector",
				},
				Components: components,
				Relations:  relations,
			}},
		}
		jsonData, err := json.Marshal(request)
		if err != nil {
			log.Error("Can't encode api request to JSON", zap.Error(err))
			return errInternal //it shouldn't happen, something is wrong with the implementation
		}

		req, err := http.NewRequest(http.MethodPost, t.cfg.Endpoint, bytes.NewReader(jsonData))
		if err != nil {
			log.Error("Can't create topology intake request ", zap.Error(err))
			return errInternal
		}
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("sts-api-key", apiKey)

		res, err := t.httpClient.Do(req)
		if err != nil {
			log.Error("Receiver endpoint returned an error ", zap.Error(err))
		}

		if res.StatusCode == 403 {
			log.Error("API Key was not valid", zap.Error(err))
		}
		log.Debug(
			fmt.Sprintf("Sent %d components for key ...%s (status %d)",
				len(components),
				apiKey[len(apiKey)-4:],
				res.StatusCode,
			),
		)
	}

	return nil
}

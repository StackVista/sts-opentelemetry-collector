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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var (
	errInternal = errors.New("internal error")
)

type TopologyExporter struct {
	logger     *zap.Logger
	httpClient http.Client
	cfg        *Config
}

func NewTopologyExporter(logger *zap.Logger, cfg component.Config) (*TopologyExporter, error) {
	stsCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config passed to stackstateexporter: %T", cfg)
	}
	httpClient := http.Client{
		Timeout: 5 * time.Second,
	}

	return &TopologyExporter{logger: logger, httpClient: httpClient, cfg: stsCfg}, nil
}

func getOrDefault(
	componentsByAPIKey map[string]*internal.ComponentsCollection,
	stsAPIKey string,
) *internal.ComponentsCollection {
	collection, hasSiblings := componentsByAPIKey[stsAPIKey]
	if !hasSiblings {
		collection = internal.NewCollection()
		componentsByAPIKey[stsAPIKey] = collection
	}
	return collection
}

func (t *TopologyExporter) logAttrs(msg string, attrs *pcommon.Map) {
	fields := make([]zap.Field, 0)
	attrs.Range(func(k string, v pcommon.Value) bool {
		fields = append(fields, zap.String(k, v.AsString()))
		return true
	})
	t.logger.Warn(msg, fields...)
}

func (t *TopologyExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	componentsByAPIKey := make(map[string]*internal.ComponentsCollection, 0)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		resource := rs.Resource()
		attrs := resource.Attributes()
		stsAPIKeyValue, keyExists := attrs.Get("sts_api_key")
		if keyExists {
			stsAPIKey := stsAPIKeyValue.AsString()
			attrs.Remove("sts_api_key")
			collection := getOrDefault(componentsByAPIKey, stsAPIKey)
			if !collection.AddResource(&attrs) {
				t.logAttrs("Skipping resource without necessary attributes", &attrs)
			}
		} else {
			// look for servicegraph metrics for relations
			ilms := rs.ScopeMetrics()
			for j := 0; j < ilms.Len(); j++ {
				ilm := ilms.At(j)
				scope := ilm.Scope()
				if scope.Name() != "traces_service_graph" {
					continue
				}

				metrics := ilm.Metrics()
				for k := 0; k < metrics.Len(); k++ {
					m := metrics.At(k)
					if m.Name() != "traces_service_graph_request_total" {
						continue
					}
					connAttrs := m.Sum().DataPoints().At(0).Attributes()
					connectionType, ok := connAttrs.Get("connection_type")
					if !ok || connectionType.AsString() == "virtual_node" {
						continue
					}

					clientAPIKeyValue, clientKeyExists := connAttrs.Get("client_sts_api_key")
					var clientAPIKey string
					if clientKeyExists {
						clientAPIKey = clientAPIKeyValue.AsString()
					}
					serverAPIKeyValue, serverKeyExists := connAttrs.Get("server_sts_api_key")
					var serverAPIKey string
					if serverKeyExists {
						serverAPIKey = serverAPIKeyValue.AsString()
					}
					if !clientKeyExists && !serverKeyExists {
						t.logAttrs("No sts_api_key attributes, found: ", &connAttrs)
						continue
					}
					connAttrs.Remove("client_sts_api_key")
					connAttrs.Remove("server_sts_api_key")
					if clientKeyExists {
						collection := getOrDefault(componentsByAPIKey, clientAPIKey)
						if !collection.AddConnection(&connAttrs) {
							t.logAttrs("Unable to add connection from servicegraphconnector to client", &connAttrs)
						}
					}
					if serverKeyExists {
						collection := getOrDefault(componentsByAPIKey, serverAPIKey)
						if !collection.AddConnection(&connAttrs) {
							t.logAttrs("Unable to add connection from servicegraphconnector to server", &connAttrs)
						}
					}
				}
				break
			}

		}
	}

	_ = t.sendCollection(componentsByAPIKey)

	return nil
}

func (t *TopologyExporter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	componentsByAPIKey := make(map[string]*internal.ComponentsCollection, 0)
	rms := td.ResourceSpans()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		resource := rs.Resource()
		attrs := resource.Attributes()
		stsAPIKeyValue, keyExists := attrs.Get("sts_api_key")
		if keyExists {
			stsAPIKey := stsAPIKeyValue.AsString()
			attrs.Remove("sts_api_key")
			collection := getOrDefault(componentsByAPIKey, stsAPIKey)
			if !collection.AddResource(&attrs) {
				t.logAttrs("Skipping resource without necessary attributes", &attrs)
			}
		}
	}

	_ = t.sendCollection(componentsByAPIKey)

	return nil
}

func (t *TopologyExporter) sendCollection(componentsByAPIKey map[string]*internal.ComponentsCollection) error {
	log := t.logger

	for apiKey, collection := range componentsByAPIKey {
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
			return errInternal // it shouldn't happen, something is wrong with the implementation
		}

		req, err := http.NewRequest(http.MethodPost, t.cfg.Endpoint, bytes.NewReader(jsonData))
		if err != nil {
			log.Error("Can't create topology intake request ", zap.Error(err))
			return errInternal
		}
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("sts-api-key", apiKey)
		req.Header.Add("sts-time-format", "ms")

		res, err := t.httpClient.Do(req)
		if err != nil {
			log.Error("Receiver endpoint returned an error ", zap.Error(err))
		}

		if res.StatusCode == 403 {
			log.Error("API Key was not valid", zap.Error(err))
		}
		if res.StatusCode < 300 {
			log.Debug(
				fmt.Sprintf("Sent %d components, %d relations for key ...%s (status %d)",
					len(components),
					len(relations),
					apiKey[len(apiKey)-4:],
					res.StatusCode,
				),
			)
		} else {
			log.Error(
				fmt.Sprintf("Failed to send %d components, %d relations for key ...%s (status %d)",
					len(components),
					len(relations),
					apiKey[len(apiKey)-4:],
					res.StatusCode,
				),
			)
		}
	}
	return nil
}

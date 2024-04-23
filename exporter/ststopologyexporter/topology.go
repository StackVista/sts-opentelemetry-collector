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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
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

type Instance struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type ComponentType struct {
	Name string `json:"name"`
}

type Component struct {
	ExternalId string            `json:"externalId"`
	Type       ComponentType     `json:"type"`
	Data       map[string]string `json:"data"`
}

type Relation struct{}

type Topology struct {
	Instance   Instance    `json:"instance"`
	Components []Component `json:"components"`
	Relations  []Relation  `json:"relations"`
}

type IntakeTopology struct {
	CollectionTimestamp int64      `json:"collection_timestamp"`
	InternalHostname    string     `json:"internalHostname"`
	Topologies          []Topology `json:"topologies"`
}

func (t *topologyExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	log := t.logger

	componentsByApiKey := make(map[string][]Component, 0)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		resource := rs.Resource()
		serviceName, ok := resource.Attributes().Get("service.name")
		if !ok {
			log.Warn("Skipping resource without service.name")
			continue
		}
		serviceNamespace, ok := resource.Attributes().Get("service.namespace")
		if !ok {
			log.Warn("Skipping resource without service.namespace")
			continue
		}
		instanceId, ok := resource.Attributes().Get("service.instance.id")
		var serviceInstanceId pcommon.Value
		if !ok {
			serviceInstanceId = serviceName
		} else {
			serviceInstanceId = instanceId
		}
		sts_api_key, key_exists := resource.Attributes().Get("sts_api_key")
		if !key_exists {
			log.Warn("No sts_api_key attribute found on resource")
			continue
		}
		components, has_siblings := componentsByApiKey[sts_api_key.AsString()]
		if !has_siblings {
			components = make([]Component, 0)
		}
		components = append(components, Component{
			fmt.Sprintf("%s-%s", serviceNamespace.AsString(), serviceName.AsString()),
			ComponentType{
				"service",
			},
			attributesToMap(rs.Resource().Attributes()),
		})
		components = append(components, Component{
			fmt.Sprintf("%s-%s-%s", serviceNamespace.AsString(), serviceName.AsString(), serviceInstanceId.AsString()),
			ComponentType{
				"service_instance",
			},
			attributesToMap(rs.Resource().Attributes()),
		})
		componentsByApiKey[sts_api_key.AsString()] = components
	}

	for apiKey, components := range componentsByApiKey {
		request := IntakeTopology{
			CollectionTimestamp: time.Now().UnixMilli(),
			InternalHostname:    "sts-otel-collector",
			Topologies: []Topology{{
				Instance: Instance{
					"otel_resource",
					"otel_collector",
				},
				Components: components,
				Relations:  make([]Relation, 0),
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
		log.Debug(fmt.Sprintf("Sent %d components for key %s (status %d)", len(components), apiKey, res.StatusCode))
	}

	return nil
}

func attributesToMap(attributes pcommon.Map) map[string]string {
	m := make(map[string]string, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

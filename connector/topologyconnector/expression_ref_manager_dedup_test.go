package topologyconnector_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	topologyConnector "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/internal"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settingsproto"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap/zaptest"
)

func TestExpressionRefManager_NestedBodyReferences(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expressions   []string
		allAttributes bool
	}{
		{name: "indexed object", expressions: []string{"log.body['object']"}},
		{name: "indexed status", expressions: []string{"log.body['object']['status']['conditions']"}},
		{name: "selected status", expressions: []string{"log.body.object.status"}},
		{name: "attributes before body", expressions: []string{"log.attributes", "log.body['object']"}, allAttributes: true},
		{name: "body before attributes", expressions: []string{"log.body['object']", "log.attributes"}, allAttributes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			variables := make([]settingsproto.OtelVariableMapping, 0, len(tc.expressions))
			for i, expression := range tc.expressions {
				variables = append(variables, settingsproto.OtelVariableMapping{
					Name: fmt.Sprintf("value%d", i), Value: aExpr(expression),
				})
			}
			mapping := settingsproto.OtelComponentMapping{
				Identifier: "declaration",
				Vars:       &variables,
			}
			manager := topologyConnector.NewExpressionRefManager(zaptest.NewLogger(t), newTestCELEvaluator(t))
			manager.Update(
				[]settingsproto.OtelInputSignal{settingsproto.LOGS},
				map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{settingsproto.LOGS: {mapping}},
				nil,
			)
			summary := manager.Current(settingsproto.LOGS, mapping.Identifier)
			require.NotNil(t, summary)
			require.Equal(t, tc.allAttributes, summary.Log.AllAttributes)
			require.Equal(t, []string{"body"}, summary.Log.FieldKeys)
		})
	}
}

func TestExpressionRefManager_DeclarationChangesBypassDeduplication(t *testing.T) {
	mapping := settingsproto.OtelComponentMapping{
		Identifier: "declaration",
		Output: settingsproto.OtelComponentMappingOutput{
			Identifier: sExpr("resource.attributes['k8s.cluster.name'] + '/' + log.attributes['k8s.object.name']"),
			Name:       sExpr("log.attributes['k8s.object.name']"),
			TypeName:   sExpr("'custom resource'"),
			Required: &settingsproto.OtelComponentMappingFieldMapping{
				Configuration: ptr(aExpr("omit(vars.object, ['status', 'metadata'])")),
			},
			Optional: &settingsproto.OtelComponentMappingFieldMapping{
				Status: ptr(aExpr("vars.status")),
			},
		},
		Vars: &[]settingsproto.OtelVariableMapping{
			{Name: "object", Value: aExpr("log.body['object']")},
			{Name: "status", Value: aExpr("log.body['object']['status']")},
		},
	}
	manager := topologyConnector.NewExpressionRefManager(zaptest.NewLogger(t), newTestCELEvaluator(t))
	manager.Update(
		[]settingsproto.OtelInputSignal{settingsproto.LOGS},
		map[settingsproto.OtelInputSignal][]settingsproto.OtelComponentMapping{settingsproto.LOGS: {mapping}},
		nil,
	)
	deduplicator := internal.NewTopologyDeduplicator(
		context.Background(), zaptest.NewLogger(t),
		internal.DeduplicationConfig{
			Enabled:         true,
			RefreshFraction: 0.5,
			CacheConfig: metrics.MeteredCacheSettings{
				Size: 100, TTL: time.Hour,
				TelemetrySettings: componenttest.NewNopTelemetrySettings(),
			},
		},
		manager,
	)
	for _, tc := range []struct {
		name               string
		generation         int
		observedGeneration int
		fieldSelector      string
		ready              string
		send               bool
	}{
		{name: "initial", generation: 3, observedGeneration: 3, fieldSelector: "metadata.name=no-such-widget", ready: "True", send: true},
		{name: "unchanged", generation: 3, observedGeneration: 3, fieldSelector: "metadata.name=no-such-widget", ready: "True"},
		{name: "configuration changed", generation: 4, observedGeneration: 3, fieldSelector: "spec.message=unsupported", ready: "True", send: true},
		{name: "status changed", generation: 4, observedGeneration: 4, fieldSelector: "spec.message=unsupported", ready: "False", send: true},
		{name: "unchanged blocked", generation: 4, observedGeneration: 4, fieldSelector: "spec.message=unsupported", ready: "False"},
		{name: "status recovered", generation: 4, observedGeneration: 4, fieldSelector: "spec.message=unsupported", ready: "True", send: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"type": "MODIFIED",
				"object": map[string]any{
					"metadata": map[string]any{"name": "manual", "generation": tc.generation},
					"spec": map[string]any{"resources": []any{
						map[string]any{"group": "example.com", "resource": "widgets", "fieldSelector": tc.fieldSelector},
					}},
					"status": map[string]any{
						"observedGeneration": tc.observedGeneration,
						"conditions": []any{
							map[string]any{"type": "Ready", "status": tc.ready, "observedGeneration": tc.observedGeneration},
						},
					},
				},
			}
			evalContext := &internal.ExpressionEvalContext{
				Resource: internal.NewResource(map[string]any{"k8s.cluster.name": "test"}),
				Log: internal.NewLog("KubernetesCustomResourceEvent", body, map[string]any{
					"k8s.object.name": "manual",
				}),
			}
			require.Equal(t, tc.send, deduplicator.ShouldSend(mapping.Identifier, settingsproto.LOGS, evalContext, 15*time.Minute))
		})
	}
}

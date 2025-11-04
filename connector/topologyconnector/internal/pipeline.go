package internal

import (
	"context"
	"fmt"

	topostreamv1 "github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/generated/topostream/topo_stream.v1"
	"github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector/metrics"
	"github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension/generated/settings"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func ConvertSpanToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	traceData ptrace.Traces,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)

	traceTraverser := NewTracesTraverser(traceData)
	baseCtx := BaseContext{
		Signal:              settings.TRACES,
		Mapper:              mapper,
		Evaluator:           eval,
		CollectionTimestamp: collectionTimestampMs,
		MetricsRecorder:     metricsRecorder,
		Results:             &result,
	}

	for _, componentMapping := range componentMappings {
		mappingCtx := &MappingContext[settings.OtelComponentMapping]{
			BaseCtx: baseCtx,
			Mapping: componentMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		traceTraverser.Traverse(ctx, v)
	}

	for _, relationMapping := range relationMappings {
		mappingCtx := &MappingContext[settings.OtelRelationMapping]{
			BaseCtx: baseCtx,
			Mapping: relationMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		traceTraverser.Traverse(ctx, v)
	}

	logResultSummary(logger, result, settings.SettingTypeOtelComponentMapping, settings.TRACES)

	return result
}

func ConvertMetricsToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	eval ExpressionEvaluator,
	mapper *Mapper,
	metricData pmetric.Metrics,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	collectionTimestampMs int64,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)

	metricsTraverser := NewMetricsTraverser(metricData)
	baseCtx := BaseContext{
		Signal:              settings.METRICS,
		Mapper:              mapper,
		Evaluator:           eval,
		CollectionTimestamp: collectionTimestampMs,
		MetricsRecorder:     metricsRecorder,
		Results:             &result,
	}

	for _, componentMapping := range componentMappings {
		mappingCtx := &MappingContext[settings.OtelComponentMapping]{
			BaseCtx: baseCtx,
			Mapping: componentMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		metricsTraverser.Traverse(ctx, v)
	}

	for _, relationMapping := range relationMappings {
		mappingCtx := &MappingContext[settings.OtelRelationMapping]{
			BaseCtx: baseCtx,
			Mapping: relationMapping,
		}
		v := NewGenericMappingVisitor(mappingCtx)
		metricsTraverser.Traverse(ctx, v)
	}

	logResultSummary(logger, result, settings.SettingTypeOtelComponentMapping, settings.TRACES)

	return result
}

func logResultSummary(
	logger *zap.Logger,
	results []MessageWithKey,
	settingType settings.SettingType,
	signal settings.OtelInputSignal,
) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	mappingsOutput := 0
	mappingErrs := 0

	for _, result := range results {
		payload := result.Message.GetPayload()

		repeatData, ok := payload.(*topostreamv1.TopologyStreamMessage_TopologyStreamRepeatElementsData)
		if !ok || repeatData.TopologyStreamRepeatElementsData == nil {
			continue
		}

		data := repeatData.TopologyStreamRepeatElementsData

		switch settingType {
		case settings.SettingTypeOtelComponentMapping:
			mappingsOutput += len(data.Components)
		case settings.SettingTypeOtelRelationMapping:
			mappingsOutput += len(data.Relations)
		}

		mappingErrs += len(data.Errors)
	}

	logger.Debug(
		"Converted signal to topology stream messages",
		zap.String("signal", string(signal)),
		zap.Int(string(settingType), mappingsOutput),
		zap.Int(fmt.Sprintf("%sErrs", string(settingType)), mappingErrs),
	)
}

func ConvertMappingRemovalsToTopologyStreamMessage(
	ctx context.Context,
	logger *zap.Logger,
	componentMappings []settings.OtelComponentMapping,
	relationMappings []settings.OtelRelationMapping,
	metricsRecorder metrics.ConnectorMetricsRecorder,
) []MessageWithKey {
	result := make([]MessageWithKey, 0)
	var componentMappingsRemoved, relationMappingsRemoved int

	for _, componentMapping := range componentMappings {
		messages := RemovalToMessageWithKey(componentMapping)
		result = append(result, messages...)
		componentMappingsRemoved++
	}

	for _, relationMapping := range relationMappings {
		messages := RemovalToMessageWithKey(relationMapping)
		result = append(result, messages...)
		relationMappingsRemoved++
	}

	if componentMappingsRemoved > 0 {
		metricsRecorder.IncMappingsRemoved(ctx, int64(componentMappingsRemoved),
			settings.SettingTypeOtelComponentMapping)
	}
	if relationMappingsRemoved > 0 {
		metricsRecorder.IncMappingsRemoved(ctx, int64(relationMappingsRemoved),
			settings.SettingTypeOtelRelationMapping)
	}

	logger.Debug(
		"Converted mapping removals to topology stream messages",
		zap.Int("componentMappingsRemoved", componentMappingsRemoved),
		zap.Int("relationMappingsRemoved", relationMappingsRemoved),
	)

	return result
}

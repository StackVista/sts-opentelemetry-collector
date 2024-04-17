// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhousestsexporter

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap/zaptest"
)

func TestExporter_pushResourcesData(t *testing.T) {
	t.Run("push success", func(t *testing.T) {
		resources := 0
		uniqueResources := map[string]bool{}
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			t.Logf("Resources %d, values:%+v", resources, values)
			if strings.HasPrefix(query, "INSERT") {
				resources++
				uniqueResources[fmt.Sprint(values[2])] = true
			}
			return nil
		})

		exporter := newTestResourceExporter(t, defaultEndpoint)
		mustWriteResourceData(t, exporter, testResources(2))
		mustWriteResourceData(t, exporter, testResources(1))

		require.Equal(t, 3, resources)
		require.Equal(t, 2, len(uniqueResources))
	})

	t.Run("check insert resources with service name and attributes", func(t *testing.T) {
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "otel_resources") {
				require.Equal(t, map[string]string{"key": "value", "service.name": "test-service-0"}, values[2])
			}
			return nil
		})

		exporter := newTestResourceExporter(t, defaultEndpoint)
		mustWriteResourceData(t, exporter, testResources(1))
	})
}

func newTestResourceExporter(t *testing.T, dsn string, fns ...func(*Config)) *resourcesExporter {
	exporter, err := newResourceExporter(zaptest.NewLogger(t), withTestExporterConfig(fns...)(dsn))
	require.NoError(t, err)
	require.NoError(t, exporter.start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.shutdown(context.TODO()) })
	return exporter
}

func testResources(count int) []pcommon.Resource {
	resources := []pcommon.Resource{}
	for i := 0; i < count; i++ {
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", fmt.Sprintf("test-service-%d", i))
		resource.Attributes().PutStr("key", "value")
		resources = append(resources, resource)
	}
	return resources
}

func mustWriteResourceData(t *testing.T, exporter *resourcesExporter, resources []pcommon.Resource) {
	resourceModels := []*resourceModel{}
	for _, resource := range resources {
		model, err := newResourceModel(resource)
		require.NoError(t, err)
		resourceModels = append(resourceModels, model)
	}
	err := exporter.InsertResources(context.TODO(), resourceModels)
	require.NoError(t, err)
}

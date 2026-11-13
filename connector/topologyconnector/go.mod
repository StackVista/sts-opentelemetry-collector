module github.com/stackvista/sts-opentelemetry-collector/connector/topologyconnector

go 1.27.4

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/google/cel-go v0.30.0
	github.com/google/go-cmp v0.7.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/oapi-codegen/oapi-codegen/v2 v2.8.2
	github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter v0.0.0
	github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension v0.0.0
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/component v1.70.0
	go.opentelemetry.io/collector/component/componenttest v0.164.0
	go.opentelemetry.io/collector/connector v0.164.0
	go.opentelemetry.io/collector/consumer v1.70.0
	go.opentelemetry.io/collector/consumer/consumertest v0.164.0
	go.opentelemetry.io/collector/pdata v1.70.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/metric v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	go.uber.org/zap v1.28.0
	google.golang.org/protobuf v1.36.12
)

require (
	cel.dev/expr v0.25.3 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/cenkalti/backoff/v7 v7.0.3 // indirect
	github.com/dprotaso/go-yit v0.0.0-20220510233725-9ba8df137936 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/getkin/kin-openapi v0.152.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v1.0.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.8 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oapi-codegen/runtime v1.8.0 // indirect
	github.com/oasdiff/yaml v0.1.3 // indirect
	github.com/oasdiff/yaml3 v0.0.16 // indirect
	github.com/onsi/gomega v1.38.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/speakeasy-api/openapi v1.24.0 // indirect
	github.com/twmb/franz-go v1.22.2 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.19.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/client v1.70.0 // indirect
	go.opentelemetry.io/collector/config/configoptional v1.70.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.70.0 // indirect
	go.opentelemetry.io/collector/confmap v1.70.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.164.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.164.0 // indirect
	go.opentelemetry.io/collector/exporter v1.70.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper v0.164.0 // indirect
	go.opentelemetry.io/collector/extension v1.70.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.164.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.70.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.164.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.164.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.164.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.164.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.70.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.164.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/exp v0.0.0-20260727155853-b88d891fe743 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/tools v0.49.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/grpc v1.84.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/stackvista/sts-opentelemetry-collector/exporter/stskafkaexporter => ../../exporter/stskafkaexporter
	github.com/stackvista/sts-opentelemetry-collector/extension/settingsproviderextension => ../../extension/settingsproviderextension
)

module github.com/stackvista/sts-opentelemetry-collector/test/logsagent

go 1.26.6

require (
	github.com/golang/snappy v1.0.0
	github.com/stackvista/sts-opentelemetry-collector/common v0.0.0
	go.opentelemetry.io/collector/confmap v1.59.0
	go.opentelemetry.io/collector/pdata v1.59.0
	go.yaml.in/yaml/v3 v3.0.4
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
)

replace github.com/stackvista/sts-opentelemetry-collector/common => ../../common

require (
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.4 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/collector/featuregate v1.59.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.1 // indirect
)

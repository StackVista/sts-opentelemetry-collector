package otlpconfigexporter

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	queueField = "sending_queue"
	sizerField = "sizer"
)

type config struct {
	upstream component.Config
	queue    *configoptional.Optional[exporterhelper.QueueBatchConfig]
}

func (c *config) Unmarshal(conf *confmap.Conf) error {
	return conf.Unmarshal(c.upstream)
}

func (c *config) Validate() error { return confmap.Validate(c.upstream) }

func (c config) Marshal(conf *confmap.Conf) error {
	if err := conf.Marshal(c.upstream); err != nil {
		return err
	}
	queue := c.queue.Get()
	if queue == nil {
		return nil
	}
	// Optional marshals a value, losing SizerType's pointer-receiver MarshalText.
	replacement := map[string]any{sizerField: queue.Sizer.String()}
	conf.Delete("sending_queue::sizer")
	if batch := queue.Batch.Get(); batch != nil {
		replacement["batch"] = map[string]any{sizerField: batch.Sizer.String()}
		conf.Delete("sending_queue::batch::sizer")
	}
	return conf.Merge(confmap.NewFromStringMap(map[string]any{queueField: replacement}))
}

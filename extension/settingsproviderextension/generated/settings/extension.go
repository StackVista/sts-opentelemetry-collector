package settings

type Mapping interface {
	GetId() string
	GetExpireAfterMs() int64
}

func (m OtelComponentMapping) GetId() string {
	return m.Id
}

func (m OtelComponentMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

func (m OtelRelationMapping) GetId() string {
	return m.Id
}

func (m OtelRelationMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

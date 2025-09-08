package settings

// NOTE: these extensions/helpers are not auto-generated

// SizeOfRawSetting returns the size of the underlying raw encoded JSON value.
// The helper exists because the union field on the Setting type is unexported.
func SizeOfRawSetting(s Setting) int64 {
	if s.union == nil {
		return 0
	}
	return int64(len(s.union))
}

type Mapping interface {
	GetID() string
	GetExpireAfterMs() int64
}

func (m OtelComponentMapping) GetID() string {
	return m.Id
}

func (m OtelComponentMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

func (m OtelRelationMapping) GetID() string {
	return m.Id
}

func (m OtelRelationMapping) GetExpireAfterMs() int64 {
	return m.ExpireAfterMs
}

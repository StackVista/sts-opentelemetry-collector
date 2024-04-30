package internal

type Instance struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type ComponentType struct {
	Name string `json:"name"`
}

type ComponentData struct {
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Layer       string            `json:"layer"`
	Domain      string            `json:"domain"`
	Environment string            `json:"environment"`
	Tags        map[string]string `json:"tags"`
}

type Component struct {
	ExternalId string         `json:"externalId"`
	Type       ComponentType  `json:"type"`
	Data       *ComponentData `json:"data"`
}

type RelationType struct {
	Name string `json:"name"`
}

type RelationData struct {
	Tags map[string]string `json:"tags"`
}

type Relation struct {
	ExternalId string        `json:"externalId"`
	SourceId   string        `json:"sourceId"`
	TargetId   string        `json:"targetId"`
	Type       RelationType  `json:"type"`
	Data       *RelationData `json:"data"`
}

type Topology struct {
	Instance   Instance     `json:"instance"`
	Components []*Component `json:"components"`
	Relations  []*Relation  `json:"relations"`
}

type IntakeTopology struct {
	CollectionTimestamp int64      `json:"collection_timestamp"`
	InternalHostname    string     `json:"internalHostname"`
	Topologies          []Topology `json:"topologies"`
}

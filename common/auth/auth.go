package auth

const StsAPIKeyHeader = "Sts-Api-Key" //nolint:gosec
const StsTenantContextKey = "Sts-Tenant"

type Data struct {
	APIKey string
}

func (a *Data) GetAPIKey() string {
	return a.APIKey
}

func (a *Data) GetAttribute(name string) any {
	switch name {
	case "apiKey":
		return a.APIKey
	default:
		return nil
	}
}

func (*Data) GetAttributeNames() []string {
	return []string{"apiKey"}
}

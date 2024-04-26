package auth

const StsAPIKeyHeader = "Sts-Api-Key"
const StsTenantContextKey = "Sts-Tenant"

type AuthData struct {
	ApiKey string
}

func (a *AuthData) GetApiKey() string {
	return a.ApiKey
}

func (a *AuthData) GetAttribute(name string) any {
	switch name {
	case "apiKey":
		return a.ApiKey
	default:
		return nil
	}
}

func (*AuthData) GetAttributeNames() []string {
	return []string{"apiKey"}
}

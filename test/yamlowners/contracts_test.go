package yamlowners_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"github.com/prometheus/prometheus/discovery/scaleway"
	"github.com/scaleway/scaleway-sdk-go/scw"
	"go.yaml.in/yaml/v2"
)

type uaGetter string

func (u uaGetter) Get(context.Context, struct{}) (string, error) { return string(u), nil }

func TestRealOTTLUserAgent(t *testing.T) {
	for _, tc := range []struct{ ua, family, version, os string }{
		{"Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0", "Firefox", "126.0", "Linux"},
		{"curl/7.81.0", "curl", "7.81.0", "Other"},
		{"foobar/1.2.3 (foo; bar baz)", "Other", "", "Other"},
	} {
		fn, err := ottlfuncs.NewUserAgentFactory[struct{}]().CreateFunction(ottl.FunctionContext{}, &ottlfuncs.UserAgentArguments[struct{}]{UserAgent: uaGetter(tc.ua)})
		if err != nil {
			t.Fatal(err)
		}
		result, err := fn(context.Background(), struct{}{})
		expected := map[string]any{"user_agent.original": tc.ua, "user_agent.name": tc.family, "user_agent.version": tc.version, "os.name": tc.os}
		if err != nil || !reflect.DeepEqual(result, expected) {
			t.Fatalf("%q: %v, %v", tc.ua, result, err)
		}
	}
}

func TestScalewayConfigClientPrecedence(t *testing.T) {
	t.Setenv(scw.ScwActiveProfileEnv, "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	fixture := "default_region: fr-par\ndefault_zone: fr-par-1\nactive_profile: chosen\nprofiles:\n  chosen:\n    default_region: nl-ams\n    default_zone: nl-ams-1\n"
	if err := os.WriteFile(path, []byte(fixture), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := scw.LoadConfigFromPath(path)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := cfg.GetActiveProfile()
	if err != nil {
		t.Fatal(err)
	}
	client, err := scw.NewClient(scw.WithProfile(profile), scw.WithoutAuth(), scw.WithDefaultZone(scw.ZoneFrPar2))
	if err != nil {
		t.Fatal(err)
	}
	zone, _ := client.GetDefaultZone()
	region, _ := client.GetDefaultRegion()
	if zone != scw.ZoneFrPar2 || region != scw.RegionNlAms {
		t.Fatalf("precedence: %v %v", zone, region)
	}
}

func TestRealPrometheusScalewayConfig(t *testing.T) {
	// Decoding calls Prometheus custom validation and constructs the real SDK
	// client. No discoverer is started and no provider request is performed.
	for _, role := range []string{"instance", "baremetal"} {
		var cfg scaleway.SDConfig
		data := "role: " + role + "\nproject_id: 11111111-1111-4111-8111-111111111111\naccess_key: SCW11111111111111111\nsecret_key: 11111111-1111-4111-8111-111111111111\n"
		if err := yaml.UnmarshalStrict([]byte(data), &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.Port != 80 || cfg.Zone != "fr-par-1" {
			t.Fatalf("defaults: %+v", cfg)
		}
		if err := yaml.UnmarshalStrict([]byte(strings.Replace(data, "role: "+role, "role: invalid", 1)), &cfg); err == nil {
			t.Fatal("invalid role accepted")
		}
		if err := yaml.UnmarshalStrict([]byte(data+"unknown_field: true\n"), &cfg); err == nil {
			t.Fatal("unknown field accepted")
		}
	}
}

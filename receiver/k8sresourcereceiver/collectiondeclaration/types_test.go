package collectiondeclaration_test

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/yaml"
)

func TestSelectionFixturesAndWireRoundTrip(t *testing.T) {
	for _, fixture := range []struct {
		name  string
		valid bool
	}{
		{"valid-selection", true},
		{"invalid-field-selector", false},
		{"invalid-group", false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + fixture.name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var object map[string]interface{}
			if err = json.Unmarshal(data, &object); err != nil {
				t.Fatal(err)
			}
			declaration, err := collectiondeclaration.Decode(&unstructured.Unstructured{Object: object})
			if err != nil {
				t.Fatal(err)
			}
			if len(declaration.Spec.Resources) != 1 {
				t.Fatal("expected one fixture selection")
			}
			selection := declaration.Spec.Resources[0]
			if err = collectiondeclaration.ValidateSelection(selection); (err == nil) != fixture.valid {
				t.Fatalf("valid=%v, validation=%v", fixture.valid, err)
			}
			if fixture.valid {
				if selection.FieldSelector != "metadata.namespace=team-a,metadata.name!=paused" {
					t.Fatalf("optional fields lost during decoding: %#v", selection)
				}
				encoded, err := json.Marshal(declaration)
				if err != nil {
					t.Fatal(err)
				}
				var roundTrip map[string]interface{}
				if err = json.Unmarshal(encoded, &roundTrip); err != nil {
					t.Fatal(err)
				}
				original, _ := json.Marshal(object["spec"])
				result, _ := json.Marshal(roundTrip["spec"])
				if string(original) != string(result) {
					t.Fatalf("spec changed: original=%s result=%s", original, result)
				}
			}
		})
	}
}

func TestSelectionFields(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		valid       bool
	}{
		{"omitted", "", true},
		{"equality", "metadata.name=web", true},
		{"double equality", "metadata.name==web", true},
		{"inequality and conjunction", "status.phase!=Pending,metadata.name=web", true},
		{"escaped values", `metadata.name=a\,b\=c\\d`, true},
		{"unsupported field remains syntactically valid", "unknown.field=value", true},
		{"empty value", "metadata.name=", true},
		{"empty clauses match Kubernetes grammar", ",metadata.name=web,", true},
		{"set operator", "metadata.name in (web)", false},
		{"missing operator", "metadata.name", false},
		{"unescaped equals", "metadata.name=a=b", false},
		{"extra equals after inequality", "metadata.name!==web", false},
		{"invalid escape", `metadata.name=a\q`, false},
		{"trailing escape", `metadata.name=a\`, false},
		{"too long field selector", "metadata.name=" + strings.Repeat("a", 2048), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selection := collectiondeclaration.Selection{Group: "collection-test.example.com", Resource: "widgets", FieldSelector: tc.field}
			if err := collectiondeclaration.ValidateSelection(selection); (err == nil) != tc.valid {
				t.Fatalf("valid=%v, validation=%v", tc.valid, err)
			}
		})
	}
}

func TestCanonicalSelectionSchema(t *testing.T) {
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(collectiondeclaration.CRD, &crd); err != nil {
		t.Fatal(err)
	}
	gvr := collectiondeclaration.GVR()
	if gvr.Group != crd.Spec.Group || gvr.Version != crd.Spec.Versions[0].Name || gvr.Resource != crd.Spec.Names.Plural {
		t.Fatal("discovery identity and canonical CRD differ")
	}
	selection := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["resources"].Items.Schema
	if len(selection.Properties) != 5 {
		t.Fatalf("expected five selection fields, got %v", selection.Properties)
	}
	for _, name := range []string{"group", "resource", "namespaces", "labelSelector", "fieldSelector"} {
		if _, exists := selection.Properties[name]; !exists {
			t.Errorf("missing field %s", name)
		}
	}
	wire := reflect.TypeOf(collectiondeclaration.Selection{})
	if wire.NumField() != len(selection.Properties) {
		t.Fatal("selection wire type and schema have different field counts")
	}
	for i := range wire.NumField() {
		name := strings.Split(wire.Field(i).Tag.Get("json"), ",")[0]
		if _, exists := selection.Properties[name]; !exists {
			t.Errorf("wire field %q is absent from the schema", name)
		}
	}
	group := selection.Properties["group"]
	if group.MinLength == nil || *group.MinLength != 1 || regexp.MustCompile(group.Pattern).MatchString("") {
		t.Fatal("empty groups must be rejected")
	}
	if _, exists := selection.Properties["version"]; exists {
		t.Fatal("declarations must not expose version pinning")
	}
	status := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties["resources"].Items.Schema
	if _, exists := status.Properties["version"]; exists {
		t.Fatal("selection status must not echo a declaration version pin")
	}
	field := selection.Properties["fieldSelector"]
	if field.MaxLength == nil || *field.MaxLength != 2048 {
		t.Fatal("field selector must be bounded")
	}
	if len(field.XValidations) != 1 || field.XValidations[0].Rule != "!self.contains('!==')" {
		t.Fatal("field-selector inequality lexical guard is missing")
	}
	pattern := regexp.MustCompile(field.Pattern)
	// Check lexical admission against the repository-pinned Kubernetes parser.
	// Actual API-server structural/CEL admission is covered separately.
	var check func(string, int)
	check = func(value string, remaining int) {
		_, err := fields.ParseSelector(value)
		admitted := pattern.MatchString(value) && !strings.Contains(value, "!==")
		if admitted != (err == nil) {
			t.Fatalf("field %q: lexical admission=%v parser=%v", value, admitted, err)
		}
		if remaining > 0 {
			for _, character := range []byte{'a', ',', '=', '!', '\\'} {
				check(value+string(character), remaining-1)
			}
		}
	}
	check("", 6)
	for _, value := range []string{
		"metadata.name=web,status.phase!=Pending",
		`metadata.name=a\,b\=c\\d`,
		`metadata.name=a\q`, `metadata.name=a\`,
		"unknown.field=value",
	} {
		check(value, 0)
	}
}

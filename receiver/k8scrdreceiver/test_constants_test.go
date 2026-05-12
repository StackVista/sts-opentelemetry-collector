//nolint:testpackage
package k8scrdreceiver

const (
	testExampleGroup       = "example.com"
	testExampleWildcard    = "*.example.com"
	testExactMatchIO       = "exact.match.io"
	testFooWildcardBar     = "foo.*.bar"
	testPoliciesWildcardIO = "policies.*.io"
	testPoliciesKubewarden = "policies.kubewarden.io"
	testSuseWildcard       = "*.suse.com"
	testInvalid            = "invalid"
	testV1Alpha1           = "v1alpha1"
	testFoosResource       = "foos"
	testTestResources      = "testresources"
	testWidgetsResource    = "widgets"
	testCRDKind            = "CustomResourceDefinition"
	testCRDListKind        = "CustomResourceDefinitionList"
	testResourceListKind   = "TestResourceList"
	testAPIVersionKey      = "apiVersion"
	testKindKey            = "kind"
	testMetadataKey        = "metadata"
	testNameKey            = "name"
	testSpecKey            = "spec"
	testGroupKey           = "group"
	testResourceVersionKey = "resourceVersion"
)

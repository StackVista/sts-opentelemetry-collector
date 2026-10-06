package k8ssanitize_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stackvista/sts-opentelemetry-collector/common/k8ssanitize"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func decoded(t *testing.T, v interface{}) string {
	t.Helper()
	s, ok := v.(string)
	require.True(t, ok)
	b, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	return string(b)
}

// mapAt returns the nested map at path.
func mapAt(t *testing.T, obj map[string]interface{}, path ...string) map[string]interface{} {
	t.Helper()
	for _, key := range path {
		next, ok := obj[key].(map[string]interface{})
		require.True(t, ok, "%s is not a map", key)
		obj = next
	}
	return obj
}

func TestSecretDataIsReplacedByItsHash(t *testing.T) {
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "Opaque",
		"metadata": map[string]interface{}{
			"name": "db",
			"annotations": map[string]interface{}{
				"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"aHVudGVyMg=="}}`,
				"team": "db",
			},
		},
		"data":       map[string]interface{}{"user": b64("admin"), "password": b64("hunter2")},
		"stringData": map[string]interface{}{"extra": "plaintext"},
	}

	k8ssanitize.Object(obj, 0)

	want := k8ssanitize.SecretDataHash(map[string][]byte{"user": []byte("admin"), "password": []byte("hunter2")})
	data := mapAt(t, obj, "data")
	assert.Len(t, data, 1)
	assert.Equal(t, want, decoded(t, data[k8ssanitize.SecretDataHashKey]))
	assert.NotContains(t, obj, "stringData")
	annotations := mapAt(t, obj, "metadata", "annotations")
	assert.Equal(t, "<redacted>", annotations["kubectl.kubernetes.io/last-applied-configuration"])
	assert.Equal(t, "db", annotations["team"])

	out, err := json.Marshal(obj)
	require.NoError(t, err)
	for _, secret := range []string{"hunter2", b64("hunter2"), "aHVudGVyMg==", "plaintext"} {
		assert.NotContains(t, string(out), secret)
	}
}

func TestSecretDataHashMatchesClusterAgent(t *testing.T) {
	// sha256 over each key then its value, keys sorted.
	assert.Equal(t, "c622a361b7fc3be29c4fdf7d13ac9434f5ab7e3919cc0efeb4dbfeec94e15ced",
		k8ssanitize.SecretDataHash(map[string][]byte{"user": []byte("admin"), "password": []byte("hunter2")}))
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", k8ssanitize.SecretDataHash(nil))
}

func TestSecretWithoutDataGetsEmptyHash(t *testing.T) {
	obj := map[string]interface{}{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]interface{}{"name": "empty"}}
	k8ssanitize.Object(obj, 0)
	assert.Equal(t, k8ssanitize.SecretDataHash(nil), decoded(t, mapAt(t, obj, "data")[k8ssanitize.SecretDataHashKey]))
}

func TestTLSSecretKeepsCertificateOnly(t *testing.T) {
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "kubernetes.io/tls",
		"metadata":   map[string]interface{}{"name": "tls"},
		"data":       map[string]interface{}{"tls.crt": b64("CERT"), "tls.key": b64("KEY")},
	}
	k8ssanitize.Object(obj, 0)
	data := mapAt(t, obj, "data")
	assert.Equal(t, b64("CERT"), data[k8ssanitize.TLSCertKey])
	assert.NotContains(t, data, "tls.key")
	assert.Len(t, data, 2)
}

func TestConfigMapDataIsSharedBetweenKeys(t *testing.T) {
	long := strings.Repeat("x", 30)
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": "cfg"},
		"data":       map[string]interface{}{"long": long, "short": "ok"},
		"binaryData": map[string]interface{}{"logo": b64("PNG")},
	}
	k8ssanitize.Object(obj, 20)

	data := mapAt(t, obj, "data")
	assert.Equal(t, long[:10]+k8ssanitize.DroppedReplacement([]byte(long[10:])), data["long"])
	assert.Equal(t, "ok", data["short"])
	binary := decoded(t, mapAt(t, obj, "binaryData")["logo"])
	assert.Equal(t, k8ssanitize.DroppedReplacement([]byte("PNG")), binary)
	assert.True(t, k8ssanitize.IsDroppedReplacement([]byte(binary)))
	assert.Equal(t, "[dropped 20 chars, hashsum: ", k8ssanitize.DroppedReplacement([]byte(long[10:]))[:28])
}

func TestConfigMapZeroSizeKeepsData(t *testing.T) {
	long := strings.Repeat("x", 30)
	obj := map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]interface{}{"long": long}}
	k8ssanitize.Object(obj, 0)
	assert.Equal(t, long, mapAt(t, obj, "data")["long"])
}

func TestOtherObjectsAreUntouched(t *testing.T) {
	for _, obj := range []map[string]interface{}{
		{"apiVersion": "vault.example.com/v1", "kind": "Secret", "data": map[string]interface{}{"k": "v"}},
		{"apiVersion": "v1", "kind": "Pod", "data": map[string]interface{}{"k": "v"}},
	} {
		k8ssanitize.Object(obj, 1)
		assert.Equal(t, map[string]interface{}{"k": "v"}, obj["data"])
	}
}

func TestMalformedContentsAreRemoved(t *testing.T) {
	obj := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]interface{}{"name": "bad", "annotations": map[string]interface{}{"a": "b"}},
		"data":       map[string]interface{}{"k": "not base64!"},
	}
	k8ssanitize.Object(obj, 0)
	assert.NotContains(t, obj, "data")
	assert.NotContains(t, obj["metadata"], "annotations")

	obj = map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "data": "not a map"}
	k8ssanitize.Object(obj, 10)
	assert.NotContains(t, obj, "data")
}

func TestIsDroppedReplacement(t *testing.T) {
	assert.True(t, k8ssanitize.IsDroppedReplacement([]byte(k8ssanitize.DroppedReplacement([]byte("anything")))))
	assert.False(t, k8ssanitize.IsDroppedReplacement([]byte("PNG")))
	assert.False(t, k8ssanitize.IsDroppedReplacement([]byte("x"+k8ssanitize.DroppedReplacement([]byte("a")))))
}

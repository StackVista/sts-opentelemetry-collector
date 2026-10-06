// Package k8ssanitize removes sensitive and oversized contents from core
// Secrets and ConfigMaps. The results reproduce the cluster agent's Secret and
// ConfigMap collectors, so topology built from them matches the cluster agent's.
package k8ssanitize

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
)

const (
	// SecretDataHashKey is the only data key of a sanitized Secret, apart from
	// a TLS Secret's public certificate.
	SecretDataHashKey = "<data hash>" //nolint:gosec // a key name, not a credential
	// TLSCertKey is kept on TLS Secrets, from which consumers derive the expiry.
	TLSCertKey = "tls.crt"

	redactedValue = "<redacted>"
	secretTypeTLS = "kubernetes.io/tls" //nolint:gosec // a type name, not a credential

	kindSecret    = "Secret"
	kindConfigMap = "ConfigMap"
)

// nolint:gochecknoglobals
var (
	secretAnnotationsToRedact = []string{
		"kubectl.kubernetes.io/last-applied-configuration",
		"openshift.io/token-secret.value",
	}
	droppedPattern = regexp.MustCompile(`^\[dropped \d+ chars, hashsum: [0-9a-f]{16}\]$`)
)

// Object sanitizes obj in place when it is a core Secret or ConfigMap. It fails
// closed: contents it cannot sanitize are removed.
func Object(obj map[string]interface{}, configMapMaxDataSize int) {
	if apiVersion, _ := obj["apiVersion"].(string); apiVersion != "v1" {
		return
	}
	var err error
	switch obj["kind"] {
	case kindSecret:
		err = secret(obj)
	case kindConfigMap:
		err = configMap(obj, configMapMaxDataSize)
	default:
		return
	}
	if err != nil {
		delete(obj, "data")
		delete(obj, "stringData")
		delete(obj, "binaryData")
		if metadata, ok := obj["metadata"].(map[string]interface{}); ok {
			delete(metadata, "annotations")
		}
	}
}

func secret(obj map[string]interface{}) error {
	raw, _, err := stringMap(obj, "data")
	if err != nil {
		return err
	}
	data := make(map[string][]byte, len(raw))
	for k, v := range raw {
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return fmt.Errorf("data %q: %w", k, err)
		}
		data[k] = decoded
	}

	sanitized := map[string]interface{}{
		SecretDataHashKey: base64.StdEncoding.EncodeToString([]byte(SecretDataHash(data))),
	}
	if secretType, _ := obj["type"].(string); secretType == secretTypeTLS {
		if cert, ok := raw[TLSCertKey]; ok {
			sanitized[TLSCertKey] = cert
		}
	}
	obj["data"] = sanitized
	delete(obj, "stringData")

	metadata, _ := obj["metadata"].(map[string]interface{})
	annotations, _ := metadata["annotations"].(map[string]interface{})
	for _, name := range secretAnnotationsToRedact {
		if _, ok := annotations[name]; ok {
			annotations[name] = redactedValue
		}
	}
	return nil
}

// SecretDataHash is the cluster agent's hash of a Secret's decoded data.
func SecretDataHash(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, k := range keys {
		hash.Write([]byte(k))
		hash.Write(data[k])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// configMap shares maxDataSize evenly between the data keys and replaces each
// value's excess, and every binary value, with its length and hash. Zero keeps
// all data.
func configMap(obj map[string]interface{}, maxDataSize int) error {
	data, _, err := stringMap(obj, "data")
	if err != nil {
		return err
	}
	if maxDataSize > 0 && len(data) > 0 {
		maxPerKey := maxDataSize / len(data)
		cut := make(map[string]interface{}, len(data))
		for k, v := range data {
			if len(v) > maxPerKey {
				v = v[:maxPerKey] + DroppedReplacement([]byte(v[maxPerKey:]))
			}
			cut[k] = v
		}
		obj["data"] = cut
	}

	binary, found, err := stringMap(obj, "binaryData")
	if err != nil {
		return err
	}
	if found {
		replaced := make(map[string]interface{}, len(binary))
		for k, v := range binary {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return fmt.Errorf("binaryData %q: %w", k, err)
			}
			replaced[k] = base64.StdEncoding.EncodeToString([]byte(DroppedReplacement(decoded)))
		}
		obj["binaryData"] = replaced
	}
	return nil
}

// DroppedReplacement stands in for content that was dropped.
func DroppedReplacement(dropped []byte) string {
	sum := sha256.Sum256(dropped)
	return fmt.Sprintf("[dropped %d chars, hashsum: %s]", len(dropped), hex.EncodeToString(sum[:])[:16])
}

// IsDroppedReplacement reports whether value is a whole DroppedReplacement.
func IsDroppedReplacement(value []byte) bool {
	return droppedPattern.Match(value)
}

func stringMap(obj map[string]interface{}, field string) (map[string]string, bool, error) {
	raw, ok := obj[field]
	if !ok || raw == nil {
		return nil, false, nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, false, fmt.Errorf("%s is not a map", field)
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, false, fmt.Errorf("%s %q is not a string", field, k)
		}
		out[k] = s
	}
	return out, true, nil
}

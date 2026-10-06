// Package k8ssanitize removes sensitive and oversized contents from core
// Secrets and ConfigMaps. The results reproduce the cluster agent's Secret and
// ConfigMap collectors, so topology built from them matches the cluster agent's.
// Unlike the cluster agent, a ConfigMap's last-applied configuration is redacted
// too, since it holds the untruncated data.
//
// Sanitizing is idempotent: client-go applies informer transforms twice when
// initialising from a streaming list.
package k8ssanitize

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	// SecretDataHashKey holds the hash that replaces a Secret's data.
	SecretDataHashKey = "<data hash>" //nolint:gosec // a key name, not a credential
	// CertificateExpirationKey holds a TLS Secret's certificate expiry, RFC 3339.
	// Neither key is a valid Secret data key, so neither can collide with data.
	CertificateExpirationKey = "<certificate expiration>"

	redactedValue = "<redacted>"
	secretTypeTLS = "kubernetes.io/tls" //nolint:gosec // a type name, not a credential
	tlsCertKey    = "tls.crt"

	kindSecret    = "Secret"
	kindConfigMap = "ConfigMap"
)

// nolint:gochecknoglobals
var (
	annotationsToRedact = []string{
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
	redactAnnotations(obj)
	delete(obj, "stringData")
	if isSanitizedSecretData(raw) {
		return nil
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
		if cert, ok := data[tlsCertKey]; ok {
			if expiration := certificateExpiration(cert); !expiration.IsZero() {
				sanitized[CertificateExpirationKey] = base64.StdEncoding.EncodeToString(
					[]byte(expiration.UTC().Format(time.RFC3339)))
			}
		}
	}
	obj["data"] = sanitized
	return nil
}

func isSanitizedSecretData(data map[string]string) bool {
	if _, ok := data[SecretDataHashKey]; !ok {
		return false
	}
	for k := range data {
		if k != SecretDataHashKey && k != CertificateExpirationKey {
			return false
		}
	}
	return true
}

func redactAnnotations(obj map[string]interface{}) {
	metadata, _ := obj["metadata"].(map[string]interface{})
	annotations, _ := metadata["annotations"].(map[string]interface{})
	for _, name := range annotationsToRedact {
		if _, ok := annotations[name]; ok {
			annotations[name] = redactedValue
		}
	}
}

// certificateExpiration is the cluster agent's: the first certificate's expiry,
// zero unless the value is a PEM certificate chain, optionally base64 encoded.
func certificateExpiration(cert []byte) time.Time {
	if !strings.HasPrefix(string(cert), "-----BEGIN CERTIFICATE-----") {
		decoded, err := base64.StdEncoding.DecodeString(string(cert))
		if err != nil {
			return time.Time{}
		}
		cert = decoded
	}
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, cert = pem.Decode(cert)
		if block == nil {
			break
		}
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return time.Time{}
		}
		certs = append(certs, parsed)
	}
	if len(certs) == 0 {
		return time.Time{}
	}
	return certs[0].NotAfter
}

// ParseCertificateExpiration reads a CertificateExpirationKey value.
func ParseCertificateExpiration(value []byte) (time.Time, error) {
	if len(value) == 0 {
		return time.Time{}, errors.New("empty certificate expiration")
	}
	return time.Parse(time.RFC3339, string(value))
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
	redactAnnotations(obj)
	if maxDataSize > 0 && len(data) > 0 {
		maxPerKey := maxDataSize / len(data)
		cut := make(map[string]interface{}, len(data))
		for k, v := range data {
			if len(v) > maxPerKey && !IsDroppedReplacement([]byte(v[maxPerKey:])) {
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
			if IsDroppedReplacement(decoded) {
				replaced[k] = v
				continue
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

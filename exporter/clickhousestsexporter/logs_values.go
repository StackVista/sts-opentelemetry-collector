package clickhousestsexporter

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type typedAttributes struct {
	strings map[string]string
	ints    map[string]int64
	doubles map[string]float64
	bools   map[string]bool
	other   map[string]string
}

func encodeLogAttributes(attrs pcommon.Map) (typedAttributes, error) {
	result := typedAttributes{
		strings: make(map[string]string),
		ints:    make(map[string]int64),
		doubles: make(map[string]float64),
		bools:   make(map[string]bool),
		other:   make(map[string]string),
	}
	seen := make(map[string]struct{}, attrs.Len())
	var err error
	attrs.Range(func(key string, value pcommon.Value) bool {
		if _, exists := seen[key]; exists {
			err = fmt.Errorf("duplicate attribute key %q", key)
			return false
		}
		seen[key] = struct{}{}
		switch value.Type() {
		case pcommon.ValueTypeStr:
			result.strings[key] = value.Str()
		case pcommon.ValueTypeInt:
			result.ints[key] = value.Int()
		case pcommon.ValueTypeDouble:
			result.doubles[key] = value.Double()
		case pcommon.ValueTypeBool:
			result.bools[key] = value.Bool()
		default:
			result.other[key], err = encodeLogAnyValue(value)
		}
		return err == nil
	})
	return result, err
}

func encodeLogAnyValue(value pcommon.Value) (string, error) {
	var encoded strings.Builder
	encoded.Grow(logAnyValueSize(value))
	if err := writeLogAnyValue(&encoded, value); err != nil {
		return "", err
	}
	return encoded.String(), nil
}

// Escaping can grow strings beyond this capacity hint.
func logAnyValueSize(value pcommon.Value) int {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		return 2
	case pcommon.ValueTypeStr:
		return len(`{"stringValue":""}`) + len(value.Str())
	case pcommon.ValueTypeInt:
		return len(`{"intValue":""}`) + 20
	case pcommon.ValueTypeDouble:
		return len(`{"doubleValue":}`) + 24
	case pcommon.ValueTypeBool:
		return len(`{"boolValue":false}`)
	case pcommon.ValueTypeBytes:
		return len(`{"bytesValue":""}`) + base64.StdEncoding.EncodedLen(value.Bytes().Len())
	case pcommon.ValueTypeSlice:
		size := len(`{"arrayValue":{"values":[]}}`)
		values := value.Slice()
		for i := 0; i < values.Len(); i++ {
			size += logAnyValueSize(values.At(i)) + 1
		}
		return size
	case pcommon.ValueTypeMap:
		size := len(`{"kvlistValue":{"values":[]}}`)
		value.Map().Range(func(key string, nested pcommon.Value) bool {
			size += len(`{"key":"","value":},`) + len(key) + logAnyValueSize(nested)
			return true
		})
		return size
	default:
		return 2
	}
}

func writeLogAnyValue(encoded *strings.Builder, value pcommon.Value) error {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		encoded.WriteString("{}")
	case pcommon.ValueTypeStr:
		encoded.WriteString(`{"stringValue":`)
		writeLogJSONString(encoded, value.Str())
		encoded.WriteByte('}')
	case pcommon.ValueTypeInt:
		encoded.WriteString(`{"intValue":"`)
		var number [20]byte
		encoded.Write(strconv.AppendInt(number[:0], value.Int(), 10))
		encoded.WriteString(`"}`)
	case pcommon.ValueTypeDouble:
		encoded.WriteString(`{"doubleValue":`)
		switch {
		case math.IsNaN(value.Double()):
			encoded.WriteString(`"NaN"`)
		case math.IsInf(value.Double(), 1):
			encoded.WriteString(`"Infinity"`)
		case math.IsInf(value.Double(), -1):
			encoded.WriteString(`"-Infinity"`)
		default:
			number, err := json.Marshal(value.Double())
			if err != nil {
				return fmt.Errorf("encode OTLP double: %w", err)
			}
			encoded.Write(number)
		}
		encoded.WriteByte('}')
	case pcommon.ValueTypeBool:
		encoded.WriteString(`{"boolValue":`)
		if value.Bool() {
			encoded.WriteString("true")
		} else {
			encoded.WriteString("false")
		}
		encoded.WriteByte('}')
	case pcommon.ValueTypeBytes:
		encoded.WriteString(`{"bytesValue":"`)
		encoded.WriteString(base64.StdEncoding.EncodeToString(value.Bytes().AsRaw()))
		encoded.WriteString(`"}`)
	case pcommon.ValueTypeSlice:
		encoded.WriteString(`{"arrayValue":{"values":[`)
		values := value.Slice()
		for i := 0; i < values.Len(); i++ {
			if i > 0 {
				encoded.WriteByte(',')
			}
			if err := writeLogAnyValue(encoded, values.At(i)); err != nil {
				return err
			}
		}
		encoded.WriteString("]}}")
	case pcommon.ValueTypeMap:
		encoded.WriteString(`{"kvlistValue":{"values":[`)
		first := true
		var err error
		value.Map().Range(func(key string, nested pcommon.Value) bool {
			if !first {
				encoded.WriteByte(',')
			}
			first = false
			encoded.WriteString(`{"key":`)
			writeLogJSONString(encoded, key)
			encoded.WriteString(`,"value":`)
			err = writeLogAnyValue(encoded, nested)
			encoded.WriteByte('}')
			return err == nil
		})
		if err != nil {
			return err
		}
		encoded.WriteString("]}}")
	default:
		return fmt.Errorf("unsupported OTLP value type %v", value.Type())
	}
	return nil
}

func writeLogJSONString(encoded *strings.Builder, value string) {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			//nolint:errchkjson // Marshaling a string cannot fail.
			escaped, _ := json.Marshal(value)
			encoded.Write(escaped)
			return
		}
	}
	encoded.WriteByte('"')
	encoded.WriteString(value)
	encoded.WriteByte('"')
}

type logResourceModel struct {
	resourceRef uuid.UUID
	attributes  typedAttributes
	authScope   []string
	serviceName string
}

type cachedLogResource struct {
	attributes pcommon.Map
	model      *logResourceModel
}

type logResourceModels struct {
	seed    maphash.Seed
	entries map[uint64]cachedLogResource
	lookups int
	hits    int
}

const logResourceCacheSampleSize = 128

func newLogResourceModels() *logResourceModels {
	return &logResourceModels{seed: maphash.MakeSeed(), entries: make(map[uint64]cachedLogResource)}
}

func (models *logResourceModels) get(resource pcommon.Resource) (*logResourceModel, error) {
	if models == nil || models.entries == nil {
		return newLogResourceModel(resource)
	}
	attrs := resource.Attributes()
	hash := models.fingerprint(attrs)
	// Compare from the validated map so duplicate incoming keys cannot hit the cache.
	if cached, found := models.entries[hash]; found && equalLogResourceAttributes(cached, attrs) {
		models.sample(true)
		return cached.model, nil
	}
	model, err := newLogResourceModel(resource)
	if err != nil {
		return nil, err
	}
	models.entries[hash] = cachedLogResource{attributes: attrs, model: model}
	models.sample(false)
	return model, nil
}

func (models *logResourceModels) sample(hit bool) {
	if models.lookups >= logResourceCacheSampleSize {
		return
	}
	models.lookups++
	if hit {
		models.hits++
	}
	// Stop paying lookup costs when the first groups show little resource reuse.
	if models.lookups == logResourceCacheSampleSize && models.hits < logResourceCacheSampleSize/4 {
		models.entries = nil
	}
}

// This lookup key does not change the canonical ResourceRef hash.
func (models *logResourceModels) fingerprint(attrs pcommon.Map) uint64 {
	var hash uint64
	attrs.Range(func(key string, value pcommon.Value) bool {
		entry := struct {
			key, text string
			number    uint64
			kind      pcommon.ValueType
		}{key: key, kind: value.Type()}
		switch value.Type() {
		case pcommon.ValueTypeStr:
			entry.text = value.Str()
		case pcommon.ValueTypeInt:
			entry.number = maphash.Comparable(models.seed, value.Int())
		case pcommon.ValueTypeDouble:
			entry.number = math.Float64bits(value.Double())
		case pcommon.ValueTypeBool:
			if value.Bool() {
				entry.number = 1
			}
		case pcommon.ValueTypeBytes:
			entry.number = maphash.Bytes(models.seed, value.Bytes().AsRaw())
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			entry.number = maphash.Comparable(models.seed, pdatautil.ValueHash(value))
		case pcommon.ValueTypeEmpty:
		}
		hash += maphash.Comparable(models.seed, entry)
		return true
	})
	return hash
}

func equalLogResourceAttributes(cached cachedLogResource, attrs pcommon.Map) bool {
	if !cached.attributes.Equal(attrs) {
		return false
	}
	equal := true
	attrs.Range(func(key string, value pcommon.Value) bool {
		if value.Type() == pcommon.ValueTypeMap || value.Type() == pcommon.ValueTypeSlice {
			// Map.Equal ignores duplicate nested keys; preserve their encoded values.
			encoded, err := encodeLogAnyValue(value)
			equal = err == nil && encoded == cached.model.attributes.other[key]
		}
		return equal
	})
	return equal
}

func normalizedLogResourceAttributes(attrs pcommon.Map) (pcommon.Map, string, error) {
	service, exists := attrs.Get("service.name")
	if !exists || (service.Type() == pcommon.ValueTypeStr && service.Str() == "") {
		normalized := pcommon.NewMap()
		attrs.CopyTo(normalized)
		attrs = normalized
		attrs.PutStr("service.name", "unknown_service")
		service, _ = attrs.Get("service.name")
	}
	if service.Type() != pcommon.ValueTypeStr {
		return pcommon.Map{}, "", fmt.Errorf("service.name must be a string")
	}
	return attrs, service.Str(), nil
}

func newLogResourceModel(resource pcommon.Resource) (*logResourceModel, error) {
	attributes, err := encodeLogAttributes(resource.Attributes())
	if err != nil {
		return nil, err
	}
	attrs, serviceName, err := normalizedLogResourceAttributes(resource.Attributes())
	if err != nil {
		return nil, err
	}
	attributes.strings["service.name"] = serviceName
	ref := logResourceRef(attrs, attributes)
	return &logResourceModel{
		resourceRef: ref,
		attributes:  attributes,
		authScope:   attributesToAuthScope(attrs),
		serviceName: serviceName,
	}, nil
}

func logResourceRef(attrs pcommon.Map, encoded typedAttributes) uuid.UUID {
	var keyBuffer [16]string
	keys := keyBuffer[:0]
	attrs.Range(func(key string, _ pcommon.Value) bool {
		keys = append(keys, key)
		return true
	})
	slices.Sort(keys)
	data := make([]byte, 0, 1024)
	data = append(data, "otel.logs.resource.v1"...)
	for _, key := range keys {
		value, _ := attrs.Get(key)
		data = binary.LittleEndian.AppendUint64(data, uint64(len(key)))
		data = append(data, key...)
		data = appendLogResourceField(data, value.Type().String())
		switch value.Type() {
		case pcommon.ValueTypeStr:
			data = appendLogResourceField(data, value.Str())
		case pcommon.ValueTypeInt:
			data = strconv.AppendInt(data, value.Int(), 10)
			data = append(data, 0)
		case pcommon.ValueTypeDouble:
			data = binary.LittleEndian.AppendUint64(data, math.Float64bits(value.Double()))
		case pcommon.ValueTypeBool:
			if value.Bool() {
				data = append(data, 1)
			} else {
				data = append(data, 0)
			}
		default:
			// Hash the stored JSON so nested duplicate keys and bytes retain their full identity.
			data = appendLogResourceField(data, encoded.other[key])
		}
	}
	hash := sha256.Sum256(data)
	return uuid.UUID(hash[:16])
}

func appendLogResourceField(data []byte, value string) []byte {
	data = binary.LittleEndian.AppendUint64(data, uint64(len(value)))
	return append(data, value...)
}

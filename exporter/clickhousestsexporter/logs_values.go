package clickhousestsexporter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
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

func newLogResourceModel(resource pcommon.Resource) (*logResourceModel, error) {
	attributes, err := encodeLogAttributes(resource.Attributes())
	if err != nil {
		return nil, err
	}
	attrs := pcommon.NewMap()
	resource.Attributes().CopyTo(attrs)
	service, exists := attrs.Get("service.name")
	if !exists || (service.Type() == pcommon.ValueTypeStr && service.Str() == "") {
		attrs.PutStr("service.name", "unknown_service")
		service, _ = attrs.Get("service.name")
	}
	if service.Type() != pcommon.ValueTypeStr {
		return nil, fmt.Errorf("service.name must be a string")
	}
	attributes.strings["service.name"] = service.Str()
	hash := pdatautil.MapHash(attrs)
	ref, err := uuid.FromBytes(hash[:])
	if err != nil {
		return nil, err
	}
	return &logResourceModel{
		resourceRef: ref,
		attributes:  attributes,
		authScope:   attributesToAuthScope(attrs),
		serviceName: service.Str(),
	}, nil
}

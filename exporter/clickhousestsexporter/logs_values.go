package clickhousestsexporter

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

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
	encoded, err := logAnyValue(value)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return "", fmt.Errorf("encode OTLP AnyValue: %w", err)
	}
	return string(data), nil
}

func logAnyValue(value pcommon.Value) (map[string]any, error) {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		return map[string]any{}, nil
	case pcommon.ValueTypeStr:
		return map[string]any{"stringValue": value.Str()}, nil
	case pcommon.ValueTypeInt:
		return map[string]any{"intValue": strconv.FormatInt(value.Int(), 10)}, nil
	case pcommon.ValueTypeDouble:
		var number any = value.Double()
		switch {
		case math.IsNaN(value.Double()):
			number = "NaN"
		case math.IsInf(value.Double(), 1):
			number = "Infinity"
		case math.IsInf(value.Double(), -1):
			number = "-Infinity"
		}
		return map[string]any{"doubleValue": number}, nil
	case pcommon.ValueTypeBool:
		return map[string]any{"boolValue": value.Bool()}, nil
	case pcommon.ValueTypeBytes:
		return map[string]any{"bytesValue": base64.StdEncoding.EncodeToString(value.Bytes().AsRaw())}, nil
	case pcommon.ValueTypeSlice:
		values := make([]any, 0, value.Slice().Len())
		for i := 0; i < value.Slice().Len(); i++ {
			nested, err := logAnyValue(value.Slice().At(i))
			if err != nil {
				return nil, err
			}
			values = append(values, nested)
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case pcommon.ValueTypeMap:
		values := make([]any, 0, value.Map().Len())
		var err error
		value.Map().Range(func(key string, nested pcommon.Value) bool {
			var encoded map[string]any
			encoded, err = logAnyValue(nested)
			if err == nil {
				values = append(values, map[string]any{"key": key, "value": encoded})
			}
			return err == nil
		})
		return map[string]any{"kvlistValue": map[string]any{"values": values}}, err
	default:
		return nil, fmt.Errorf("unsupported OTLP value type %v", value.Type())
	}
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

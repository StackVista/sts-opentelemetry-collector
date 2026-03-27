package internal

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// celListToStringSet converts a CEL list value to a set of string keys.
// Returns a non-nil error ref.Val on failure (to be returned directly from CEL bindings).
func celListToStringSet(listVal ref.Val) (map[string]struct{}, ref.Val) {
	lister, ok := listVal.(traits.Lister)
	if !ok {
		return nil, types.WrapErr(fmt.Errorf("pick/omit: second argument must be a list, got %T", listVal))
	}
	keys := make(map[string]struct{})
	it := lister.Iterator()
	for it.HasNext() == types.True {
		elem := it.Next()
		s, ok := elem.(types.String)
		if !ok {
			return nil, types.WrapErr(fmt.Errorf("pick/omit: key list elements must be strings, got %T", elem))
		}
		keys[string(s)] = struct{}{}
	}
	return keys, nil
}

// celPickImpl implements the CEL pick(map, list) function.
// It returns a new map containing only the keys specified in the list.
// Keys absent from the map are silently ignored.
func celPickImpl(mapVal, listVal ref.Val) ref.Val {
	mapper, ok := mapVal.(traits.Mapper)
	if !ok {
		return types.WrapErr(fmt.Errorf("pick: first argument must be a map, got %T", mapVal))
	}
	keys, errVal := celListToStringSet(listVal)
	if errVal != nil {
		return errVal
	}
	result := make(map[ref.Val]ref.Val, len(keys))
	for k := range keys {
		keyVal := types.String(k)
		if val, found := mapper.Find(keyVal); found {
			result[keyVal] = val
		}
		// absent keys are silently ignored
	}
	return types.DefaultTypeAdapter.NativeToValue(result)
}

// celOmitImpl implements the CEL omit(map, list) function.
// It returns a new map with the specified keys removed.
// Keys absent from the map are silently ignored.
func celOmitImpl(mapVal, listVal ref.Val) ref.Val {
	mapper, ok := mapVal.(traits.Mapper)
	if !ok {
		return types.WrapErr(fmt.Errorf("omit: first argument must be a map, got %T", mapVal))
	}
	omitSet, errVal := celListToStringSet(listVal)
	if errVal != nil {
		return errVal
	}
	result := make(map[ref.Val]ref.Val)
	it := mapper.Iterator()
	for it.HasNext() == types.True {
		keyVal := it.Next()
		s, ok := keyVal.(types.String)
		if !ok {
			return types.WrapErr(fmt.Errorf("omit: key must be a string, got %T", keyVal))
		}
		if _, shouldOmit := omitSet[string(s)]; !shouldOmit {
			val, _ := mapper.Find(keyVal)
			result[keyVal] = val
		}
	}
	return types.DefaultTypeAdapter.NativeToValue(result)
}

// celCustomFunctions returns CEL environment options that register the pick and omit functions.
func celCustomFunctions() []cel.EnvOption {
	mapStringDyn := cel.MapType(cel.StringType, cel.DynType)
	listStringType := cel.ListType(cel.StringType)
	return []cel.EnvOption{
		cel.Function("pick",
			cel.Overload("pick_map_list",
				[]*cel.Type{mapStringDyn, listStringType}, mapStringDyn,
				cel.BinaryBinding(celPickImpl),
			),
		),
		cel.Function("omit",
			cel.Overload("omit_map_list",
				[]*cel.Type{mapStringDyn, listStringType}, mapStringDyn,
				cel.BinaryBinding(celOmitImpl),
			),
		),
	}
}

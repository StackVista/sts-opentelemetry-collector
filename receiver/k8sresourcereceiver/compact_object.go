package k8sresourcereceiver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/tools/cache"
)

// resourceObject is an object held by the informer and resource caches: a
// *unstructured.Unstructured, or a *compactObject when CompactCache is on.
// Only the emit path needs the full object; see decodeObject.
type resourceObject interface {
	GetName() string
	GetNamespace() string
	GetResourceVersion() string
	GetKind() string
}

var (
	_ resourceObject = (*unstructured.Unstructured)(nil)
	_ resourceObject = (*compactObject)(nil)
	_ runtime.Object = (*compactObject)(nil)
	_ metav1.Object  = (*compactObject)(nil)
)

// compactObject holds a sanitized object as compact JSON, which retains far
// less memory than its decoded map. The metadata it keeps is what informer
// keying, change detection and payload budgets read without decoding.
type compactObject struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	raw []byte
	// size is the object's json.Marshal length, which payload budgets use.
	size int
}

func newCompactObject(u *unstructured.Unstructured) (*compactObject, error) {
	// Same encoding as serializedObjectSize, so payload budgets see equal sizes.
	raw, err := json.Marshal(u.Object)
	if err != nil {
		return nil, err
	}
	size := len(raw)
	if hasIntegralFloat(u.Object) {
		// json.Marshal writes 1.0 as 1, which decodes as an integer.
		if raw, err = json.Marshal(keepFloats(u.Object)); err != nil {
			return nil, err
		}
	}
	return &compactObject{
		TypeMeta: metav1.TypeMeta{APIVersion: u.GetAPIVersion(), Kind: u.GetKind()},
		ObjectMeta: metav1.ObjectMeta{
			Name:            u.GetName(),
			Namespace:       u.GetNamespace(),
			UID:             u.GetUID(),
			ResourceVersion: u.GetResourceVersion(),
		},
		raw:  raw,
		size: size,
	}, nil
}

// compactObjectFromJSON takes ownership of raw.
func compactObjectFromJSON(raw []byte) (*compactObject, error) {
	var header struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name            string    `json:"name"`
			Namespace       string    `json:"namespace"`
			UID             types.UID `json:"uid"`
			ResourceVersion string    `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	return &compactObject{
		TypeMeta: metav1.TypeMeta{APIVersion: header.APIVersion, Kind: header.Kind},
		ObjectMeta: metav1.ObjectMeta{
			Name:            header.Metadata.Name,
			Namespace:       header.Metadata.Namespace,
			UID:             header.Metadata.UID,
			ResourceVersion: header.Metadata.ResourceVersion,
		},
		raw:  raw,
		size: len(raw),
	}, nil
}

func (c *compactObject) GetKind() string { return c.Kind }

func (c *compactObject) DeepCopyObject() runtime.Object {
	meta := c.ObjectMeta
	return &compactObject{TypeMeta: c.TypeMeta, ObjectMeta: *meta.DeepCopy(), raw: bytes.Clone(c.raw), size: c.size}
}

// decode uses the dynamic client's JSON number handling, so the result equals
// the object the informer would otherwise have cached.
func (c *compactObject) decode() (*unstructured.Unstructured, error) {
	obj := map[string]interface{}{}
	if err := utiljson.Unmarshal(c.raw, &obj); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

// decodeObject returns obj as an Unstructured, decoding compact entries.
func decodeObject(obj resourceObject) (*unstructured.Unstructured, error) {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return o, nil
	case *compactObject:
		return o.decode()
	default:
		return nil, fmt.Errorf("unexpected cached object type %T", obj)
	}
}

func toResourceObject(obj interface{}) (resourceObject, bool) {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return o, true
	case *compactObject:
		return o, true
	default:
		return nil, false
	}
}

func objectPayloadSize(obj resourceObject) (int, bool) {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return serializedObjectSize(o.Object)
	case *compactObject:
		return o.size, true
	default:
		return 0, false
	}
}

// marshalWireObject encodes obj for peer sync. Both representations produce
// the same wire format, so replicas interoperate whatever their CompactCache.
func marshalWireObject(obj resourceObject) (json.RawMessage, error) {
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return o.MarshalJSON()
	case *compactObject:
		return o.raw, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected cached object type %T", obj)
	}
}

func unmarshalWireObject(raw json.RawMessage, compact bool) (resourceObject, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, errors.New("missing obj")
	}
	if compact {
		return compactObjectFromJSON(raw)
	}
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	return u, nil
}

// compactTransform runs sanitize and then compacts the result. It never errors,
// because a transform error would stall the informer: an object that cannot be
// encoded stays live, which every cache consumer accepts.
func compactTransform(sanitize cache.TransformFunc) cache.TransformFunc {
	return func(obj interface{}) (interface{}, error) {
		obj, _ = sanitize(obj)
		// Already-compact objects pass through: client-go transforms streamed
		// list items twice.
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		if c, err := newCompactObject(u); err == nil {
			return c, nil
		}
		return u, nil
	}
}

// integralFloat reports whether json.Marshal writes f without a fraction or
// exponent, as it does for 1.0 below 1e21.
func integralFloat(f float64) bool {
	return f == math.Trunc(f) && math.Abs(f) < 1e21
}

func hasIntegralFloat(v interface{}) bool {
	switch t := v.(type) {
	case float64:
		return integralFloat(t)
	case map[string]interface{}:
		for _, e := range t {
			if hasIntegralFloat(e) {
				return true
			}
		}
	case []interface{}:
		for _, e := range t {
			if hasIntegralFloat(e) {
				return true
			}
		}
	}
	return false
}

// keepFloats copies v with integral floats as json.Number "1.0", which decodes
// back to a float.
func keepFloats(v interface{}) interface{} {
	switch t := v.(type) {
	case float64:
		if integralFloat(t) {
			return json.Number(strconv.FormatFloat(t, 'f', -1, 64) + ".0")
		}
		return t
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, e := range t {
			out[k] = keepFloats(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = keepFloats(e)
		}
		return out
	}
	return v
}

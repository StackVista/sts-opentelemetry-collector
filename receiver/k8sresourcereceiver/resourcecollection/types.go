package resourcecollection

import (
	_ "embed"
	"fmt"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	Group   = "observability.suse.com"
	Version = "v1alpha1"
	Kind    = "ResourceCollection"

	Active  = "Active"
	Pending = "Pending"
	Blocked = "Blocked"

	MaxSelections = 32
)

func GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: Version, Resource: "resourcecollections"}
}

//go:embed resourcecollection-crd.yaml
var CRD []byte

type ResourceCollection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              Spec   `json:"spec"`
	Status            Status `json:"status,omitempty"`
}

type Spec struct {
	Resources []Selection `json:"resources"`
}

type Selection struct {
	Group         string                `json:"group"`
	Resource      string                `json:"resource"`
	Namespaces    []string              `json:"namespaces,omitempty"`
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	FieldSelector string                `json:"fieldSelector,omitempty"`
}

type Status struct {
	ObservedGeneration int64              `json:"observedGeneration"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Resources          []SelectionStatus  `json:"resources,omitempty"`
}

type SelectionStatus struct {
	SelectionIndex int    `json:"selectionIndex"`
	Group          string `json:"group"`
	Resource       string `json:"resource"`
	State          string `json:"state"`
	Reason         string `json:"reason"`
	Message        string `json:"message,omitempty"`
}

func Decode(obj *unstructured.Unstructured) (*ResourceCollection, error) {
	var result ResourceCollection
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &result); err != nil {
		return nil, fmt.Errorf("decode ResourceCollection: %w", err)
	}
	return &result, nil
}

func ValidateSelection(s Selection) error {
	if errs := validation.IsDNS1123Subdomain(s.Group); len(errs) != 0 {
		return fmt.Errorf("invalid custom-resource group: %v", errs)
	}
	if s.Resource != "*" {
		if errs := validation.IsDNS1035Label(s.Resource); len(errs) != 0 {
			return fmt.Errorf("invalid resource: %v", errs)
		}
	}
	if len(s.Namespaces) > 128 {
		return fmt.Errorf("too many namespaces")
	}
	for _, ns := range s.Namespaces {
		if errs := validation.IsDNS1123Label(ns); len(errs) != 0 {
			return fmt.Errorf("invalid namespace: %v", errs)
		}
	}
	if s.LabelSelector != nil {
		if _, err := metav1.LabelSelectorAsSelector(s.LabelSelector); err != nil {
			return fmt.Errorf("invalid label selector: %w", err)
		}
	}
	if utf8.RuneCountInString(s.FieldSelector) > 2048 {
		return fmt.Errorf("field selector exceeds 2048 characters")
	}
	if _, err := fields.ParseSelector(s.FieldSelector); err != nil {
		return fmt.Errorf("invalid field selector: %w", err)
	}
	return nil
}

// The StackState Processor is a custom processor that adds StackState specific attributes to the trace, log or metric.
// It is intended to be used in conjunction with the StackState Exporter, so that the right attributes are added to the trace and StackState can correctly link the data to the Kubernetes resources.
//
//go:generate mdatagen metadata.yaml
package stackstateprocessor

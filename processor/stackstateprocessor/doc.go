// The StackState Processor is a custom processor that adds StackState specific attributes to the trace.
// It is intended to be used in conjunction with the StackState Exporter, so that the right attributes are added to the trace and StackState can correctly link the data to the Kubernetes resources.
// Note: The processor will only be able to add the required attributes if the k8sattributesprocessor is added to the pipeline before the StackState processor.
//
//go:generate mdatagen metadata.yaml
package stackstateprocessor

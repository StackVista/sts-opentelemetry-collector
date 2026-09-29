package stsk8slogsexporter

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/protobuf/encoding/protowire"
)

type invalidReason string

const (
	invalidCluster   invalidReason = "cluster"
	invalidIdentity  invalidReason = "identity"
	invalidTimestamp invalidReason = "timestamp"
	invalidBody      invalidReason = "body"
	invalidStream    invalidReason = "stream"
)

type encodedLogs struct {
	payload []byte
	records int
	invalid map[invalidReason]int
}

type encoder struct{ clusterName string }

func newEncoder(clusterName string) (*encoder, error) {
	if err := validateClusterName(clusterName); err != nil {
		return nil, err
	}
	return &encoder{clusterName: clusterName}, nil
}

var (
	podNamePattern       = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	containerNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

type streamIdentity struct {
	podUID, podName, containerName, stream string
}

type logStream struct {
	identity streamIdentity
	entries  []byte
}

func (e *encoder) encode(logs plog.Logs) encodedLogs {
	result := encodedLogs{invalid: make(map[invalidReason]int)}
	var streams []logStream
	index := make(map[streamIdentity]int)
	resources := logs.ResourceLogs()
	for i := 0; i < resources.Len(); i++ {
		resource := resources.At(i)
		identity, resourceError := e.resourceIdentity(resource.Resource().Attributes())
		scopes := resource.ScopeLogs()
		for j := 0; j < scopes.Len(); j++ {
			records := scopes.At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				record := records.At(k)
				if resourceError != "" {
					result.invalid[resourceError]++
					continue
				}
				reason := validateRecord(record)
				if reason != "" {
					result.invalid[reason]++
					continue
				}
				identity.stream, _ = stringAttribute(record.Attributes(), "log.iostream")
				streamIndex, exists := index[identity]
				if !exists {
					streamIndex = len(streams)
					index[identity] = streamIndex
					streams = append(streams, logStream{identity: identity})
				}
				streams[streamIndex].entries = appendMessage(streams[streamIndex].entries, 2, encodeEntry(record))
				result.records++
			}
		}
	}
	if result.records == 0 {
		return result
	}
	var request []byte
	for _, stream := range streams {
		labels := fmt.Sprintf(`{sts_cluster_name="%s",pod_uid="%s",pod_name="%s",container_name="%s"`,
			e.clusterName, stream.identity.podUID, stream.identity.podName, stream.identity.containerName)
		if stream.identity.stream != "" {
			labels += `,stream="` + stream.identity.stream + `"`
		}
		labels += "}"
		message := appendMessage(nil, 1, []byte(labels))
		message = append(message, stream.entries...)
		request = appendMessage(request, 1, message)
	}
	result.payload = snappy.Encode(nil, request)
	return result
}

func (e *encoder) resourceIdentity(attrs pcommon.Map) (streamIdentity, invalidReason) {
	if cluster, exists := attrs.Get("k8s.cluster.name"); exists &&
		(cluster.Type() != pcommon.ValueTypeStr || cluster.Str() != e.clusterName) {
		return streamIdentity{}, invalidCluster
	}
	podUID, uidOK := stringAttribute(attrs, "k8s.pod.uid")
	podName, podOK := stringAttribute(attrs, "k8s.pod.name")
	containerName, containerOK := stringAttribute(attrs, "k8s.container.name")
	if !uidOK || !podOK || !containerOK || len(podUID) != 36 ||
		len(podName) > 253 || !podNamePattern.MatchString(podName) ||
		len(containerName) > 63 || !containerNamePattern.MatchString(containerName) {
		return streamIdentity{}, invalidIdentity
	}
	parsed, err := uuid.Parse(podUID)
	if err != nil || !strings.EqualFold(parsed.String(), podUID) {
		return streamIdentity{}, invalidIdentity
	}
	return streamIdentity{podUID: parsed.String(), podName: podName, containerName: containerName}, ""
}

func stringAttribute(attrs pcommon.Map, key string) (string, bool) {
	value, exists := attrs.Get(key)
	if !exists || value.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	return value.Str(), true
}

func validateRecord(record plog.LogRecord) invalidReason {
	// OTel zero means an absent event timestamp; observed time is not a substitute.
	if record.Timestamp() == 0 {
		return invalidTimestamp
	}
	if record.Body().Type() != pcommon.ValueTypeStr || !utf8.ValidString(record.Body().Str()) {
		return invalidBody
	}
	if stream, exists := record.Attributes().Get("log.iostream"); exists &&
		(stream.Type() != pcommon.ValueTypeStr || (stream.Str() != "stdout" && stream.Str() != "stderr")) {
		return invalidStream
	}
	return ""
}

func encodeEntry(record plog.LogRecord) []byte {
	// Divide the unsigned OTel timestamp before converting; AsTime wraps above 2262.
	nanos := uint64(record.Timestamp())
	timestamp := protowire.AppendTag(nil, 1, protowire.VarintType)
	timestamp = protowire.AppendVarint(timestamp, nanos/1_000_000_000)
	timestamp = protowire.AppendTag(timestamp, 2, protowire.VarintType)
	timestamp = protowire.AppendVarint(timestamp, nanos%1_000_000_000)
	entry := appendMessage(nil, 1, timestamp)
	return appendMessage(entry, 2, []byte(record.Body().Str()))
}

func appendMessage(dst []byte, field protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, field, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

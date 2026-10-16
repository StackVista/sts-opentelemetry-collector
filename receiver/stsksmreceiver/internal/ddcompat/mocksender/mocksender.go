// Package mocksender is a testify-based recorder with the subset of the agent
// mocksender API that the vendored KSM tests use.
package mocksender

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/stsksmreceiver/internal/ddcompat/servicecheck"
)

type MockSender struct {
	mock.Mock
}

func NewMockSender(_ string) *MockSender {
	return &MockSender{}
}

func (m *MockSender) Gauge(metric string, value float64, hostname string, tags []string) {
	m.Called(metric, value, hostname, slices.Clone(tags))
}

func (m *MockSender) ServiceCheck(checkName string, status servicecheck.ServiceCheckStatus, hostname string, tags []string, message string) {
	m.Called(checkName, status, hostname, slices.Clone(tags), message)
}

func (m *MockSender) SetupAcceptAll() {
	m.On("Gauge", mock.AnythingOfType("string"), mock.AnythingOfType("float64"), mock.AnythingOfType("string"), mock.AnythingOfType("[]string")).Return()
	m.On("ServiceCheck", mock.AnythingOfType("string"), mock.AnythingOfType("servicecheck.ServiceCheckStatus"), mock.AnythingOfType("string"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string")).Return()
}

func (m *MockSender) AssertMetric(t *testing.T, method string, metric string, value float64, hostname string, tags []string) bool {
	return m.Mock.AssertCalled(t, method, metric, value, hostname, MatchTagsContains(tags))
}

func (m *MockSender) AssertMetricNotTaggedWith(t *testing.T, method string, metric string, tags []string) bool {
	return m.Mock.AssertNotCalled(t, method, metric, mock.AnythingOfType("float64"), mock.AnythingOfType("string"), MatchTagsContains(tags))
}

func (m *MockSender) AssertServiceCheck(t *testing.T, checkName string, status servicecheck.ServiceCheckStatus, hostname string, tags []string, message string) bool {
	okCall := m.Mock.AssertCalled(t, "ServiceCheck", checkName, status, hostname, MatchTagsContains(tags), message)
	notOkCalls := m.Mock.AssertNotCalled(t, "ServiceCheck", checkName, AnythingBut(status), hostname, MatchTagsContains(tags), mock.AnythingOfType("string"))
	return okCall && notOkCalls
}

func AnythingBut(expected any) any {
	return mock.MatchedBy(func(actual any) bool {
		return !assert.ObjectsAreEqualValues(expected, actual)
	})
}

func MatchTagsContains(expected []string) any {
	return mock.MatchedBy(func(actual []string) bool {
		for _, e := range expected {
			if !slices.Contains(actual, e) {
				return false
			}
		}
		return true
	})
}

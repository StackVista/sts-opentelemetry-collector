package stsapitokenextension

import (
	"net/http"
)

// StsAPITokenRoundTripper intercepts and adds Bearer token Authorization headers to each http request.
type StsAPITokenRoundTripper struct {
	baseTransport  http.RoundTripper
	apiTokenHeader string
	apiTokenFunc   func() string
}

// RoundTrip modifies the original request and adds Bearer token Authorization headers.
func (interceptor *StsAPITokenRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	if req2.Header == nil {
		req2.Header = make(http.Header)
	}

	req2.Header.Set(interceptor.apiTokenHeader, interceptor.apiTokenFunc())

	return interceptor.baseTransport.RoundTrip(req2)
}

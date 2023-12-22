// The STS Authentication Extension is used as a server side authenticator to validate the incoming requests for a valid StackState API Token.
// If the incoming token is valid, the STS Authentication Extension will add a tenant id belonging to the API Token to the context of the incoming request.
//
//go:generate mdatagen metadata.yaml
package stsauthenticationextension

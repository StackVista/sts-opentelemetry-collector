// Package util holds agent helpers used by the vendored collectors.
package util

import "strings"

// ExtractLastFragment extracts the last fragment of a path
func ExtractLastFragment(value string) string {
	lastSlash := strings.LastIndex(value, "/")
	return value[lastSlash+1:]
}

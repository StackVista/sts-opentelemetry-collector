package stsk8slogsexporter

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validateClusterName(name string) error {
	if strings.TrimSpace(name) == "" || !utf8.ValidString(name) {
		return errors.New("cluster_name must be a nonempty UTF-8 string")
	}
	// The legacy Receiver strips label quotes without decoding escape sequences.
	if strings.ContainsAny(name, "\"\\") || strings.ContainsFunc(name, func(r rune) bool { return !unicode.IsPrint(r) }) {
		return errors.New("cluster_name must not require escaping in legacy log labels: " +
			"double quotes, backslashes and non-printable characters are unsupported")
	}
	return nil
}

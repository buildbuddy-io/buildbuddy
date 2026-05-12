// Package test_data provides embedded test data for the parser package,
// including captured Bazel help output in various formats for use in parser
// tests and validation.
package test_data

import _ "embed"

//go:embed bazel7.4.0_help_flags-as-proto.b64
var BazelHelpFlagsAsProtoOutput string

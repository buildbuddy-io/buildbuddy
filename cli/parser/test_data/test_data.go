package test_data

import _ "embed"

//go:embed bazel5.3.1_help_startup_options.txt
var BazelHelpStartupOptionsOutput string

//go:embed bazel5.3.1_help_build.txt
var BazelHelpBuildOutput string

//go:embed bazel5.3.1_help_run.txt
var BazelHelpRunOutput string

//go:embed bazel5.3.1_help_test.txt
var BazelHelpTestOutput string

//go:embed bazel5.3.1_help_query.txt
var BazelHelpQueryOutput string

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "parser",
    srcs = ["parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser",
    deps = [
        "//cli/bazelisk",
        "//cli/cli_command",
        "//cli/help/option_definitions",
        "//cli/log",
        "//cli/log/option_definitions",
        "//cli/parser/arguments",
        "//cli/parser/bazelrc",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "//cli/shortcuts",
        "//cli/storage",
        "//cli/stream_run_logs/option_definitions",
        "//cli/watcher/option_definitions",
        "//cli/workspace",
        "//proto:bazel_flags_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/lib/set",
        "//server/util/proto",
        "//server/util/status",
    ],
)

go_test(
    name = "parser_test",
    srcs = ["parser_test.go"],
    embed = [":parser"],
    deps = [
        "//cli/log",
        "//cli/parser/test_data",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

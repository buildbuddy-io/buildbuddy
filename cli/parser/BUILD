load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "parser",
    srcs = ["parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser",
    deps = [
        "//cli/bazelisk",
        "//cli/log",
        "//cli/log/option_definitions",
        "//cli/parser/arguments",
        "//cli/parser/bazelrc",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "//cli/storage",
        "//cli/watcher/option_definitions",
        "//cli/workspace",
        "//proto:bazel_flags_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/lib/set",
        "//server/util/proto",
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

package(default_visibility = ["//cli:__subpackages__"])

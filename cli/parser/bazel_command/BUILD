load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:default_visibility //cli:__subpackages__,//enterprise:__subpackages__,//server:__subpackages__
package(default_visibility = [
    "//cli:__subpackages__",
    "//enterprise:__subpackages__",
    "//server:__subpackages__",
])

go_library(
    name = "bazel_command",
    srcs = ["bazel_command.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bazel_command",
    deps = [
        "//cli/cli_command",
        "//server/util/lib/set",
    ],
)

go_test(
    name = "bazel_command_test",
    srcs = ["bazel_command_test.go"],
    deps = [
        ":bazel_command",
        "@com_github_stretchr_testify//assert",
    ],
)

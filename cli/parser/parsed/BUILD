load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "parsed",
    srcs = ["parsed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/parsed",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/parser/arguments",
        "//cli/parser/bazelrc",
        "//cli/parser/options",
        "//server/util/lib/set",
        "//server/util/shlex",
    ],
)

go_test(
    name = "parsed_test",
    srcs = ["parsed_test.go"],
    deps = [
        ":parsed",
        "//cli/parser/arguments",
        "//cli/parser/options",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

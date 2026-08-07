load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bazelrc",
    srcs = ["bazelrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/parser/bazel_command",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "//cli/parser/rc_util",
        "//server/util/lib/set",
        "@com_github_google_shlex//:shlex",
    ],
)

go_test(
    name = "bazelrc_test",
    srcs = ["bazelrc_test.go"],
    deps = [
        ":bazelrc",
        "//cli/parser/arguments",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "@com_github_stretchr_testify//require",
    ],
)

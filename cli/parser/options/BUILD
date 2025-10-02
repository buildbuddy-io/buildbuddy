load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "options",
    srcs = ["options.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/options",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/parser/arguments",
        "//cli/parser/bazelrc",
        "//cli/parser/options/flag_form",
        "//proto:bazel_flags_go_proto",
        "//server/util/lib/seq",
        "//server/util/lib/set",
    ],
)

go_test(
    name = "options_test",
    srcs = ["options_test.go"],
    deps = [
        ":options",
        "//cli/parser/options/flag_form",
        "@com_github_stretchr_testify//assert",
    ],
)

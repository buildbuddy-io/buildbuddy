load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "iprules_util",
    srcs = ["iprules_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/iprules_util",
    deps = [
        "//server/interfaces",
        "//server/util/lru",
    ],
)

go_test(
    name = "iprules_util_test",
    srcs = ["iprules_util_test.go"],
    deps = [
        ":iprules_util",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

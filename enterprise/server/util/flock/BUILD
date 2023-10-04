load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "flock",
    srcs = ["flock.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/flock",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "flock_test",
    srcs = ["flock_test.go"],
    deps = [
        ":flock",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testexecutor",
    srcs = ["testexecutor.go"],
    data = [
        "//enterprise/server/cmd/executor",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor",
    deps = [
        "//server/testutil/testport",
        "//server/testutil/testserver",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testcontainer",
    testonly = 1,
    srcs = ["testcontainer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testcontainer",
    deps = ["//server/interfaces"],
)

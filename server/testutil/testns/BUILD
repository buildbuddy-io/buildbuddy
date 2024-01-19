load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "testns",
    testonly = 1,
    srcs = ["testns.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testns",
    target_compatible_with = ["@platforms//os:linux"],
)

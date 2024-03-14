load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testmount",
    testonly = 1,
    srcs = ["testmount.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testmount",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
)

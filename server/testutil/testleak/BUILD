load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testleak",
    testonly = 1,
    srcs = ["testleak.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testleak",
    visibility = ["//visibility:public"],
    deps = ["@org_uber_go_goleak//:goleak"],
)

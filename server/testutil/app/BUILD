load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    testonly = 1,
    srcs = ["app.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/app",
    visibility = ["//visibility:public"],
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

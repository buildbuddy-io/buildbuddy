load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "buildbuddy",
    testonly = 1,
    srcs = ["buildbuddy.go"],
    data = [
        "//:config/buildbuddy.local.yaml",
        "//server/cmd/buildbuddy",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy",
    visibility = ["//visibility:public"],
    deps = ["//server/testutil/app"],
)

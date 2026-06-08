load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testusage",
    testonly = 1,
    srcs = ["testusage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testusage",
    visibility = ["//visibility:public"],
    deps = [
        "//server/tables",
        "//server/usage/sku",
        "//server/util/claims",
        "//server/util/usageutil",
    ],
)

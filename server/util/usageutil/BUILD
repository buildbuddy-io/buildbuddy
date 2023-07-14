load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "usageutil",
    srcs = ["usageutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/usageutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/tables",
        "//server/util/bazel_request",
        "//server/util/clientip",
        "//server/util/flagutil",
        "//server/util/log",
        "@org_golang_google_grpc//metadata",
    ],
)

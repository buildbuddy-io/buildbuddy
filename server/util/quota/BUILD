load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "quota",
    srcs = ["quota.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/quota",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

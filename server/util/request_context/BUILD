load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "request_context",
    srcs = ["request_context.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/request_context",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:context_go_proto",
        "@org_golang_google_protobuf//proto",
    ],
)

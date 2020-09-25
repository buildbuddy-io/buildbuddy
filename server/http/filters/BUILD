load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["filters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/filters",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/http/protolet:go_default_library",
        "//server/util/log:go_default_library",
        "//server/util/uuid:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

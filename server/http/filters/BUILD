load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "filters",
    srcs = ["filters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/filters",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:context_go_proto",
        "//server/environment",
        "//server/http/protolet",
        "//server/metrics",
        "//server/role_filter",
        "//server/util/log",
        "//server/util/request_context",
        "//server/util/uuid",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

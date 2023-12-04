load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "util",
    srcs = ["util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/util",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/metrics",
        "//server/util/ioutil",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//status",
    ],
)

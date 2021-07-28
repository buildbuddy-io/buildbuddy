load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "dirtools",
    srcs = ["dirtools.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

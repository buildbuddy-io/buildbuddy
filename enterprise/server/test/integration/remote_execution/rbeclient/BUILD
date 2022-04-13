load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "rbeclient",
    srcs = ["rbeclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbeclient",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

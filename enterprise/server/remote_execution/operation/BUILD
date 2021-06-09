load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "operation",
    srcs = ["operation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

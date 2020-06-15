load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["operation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_execution/operation",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

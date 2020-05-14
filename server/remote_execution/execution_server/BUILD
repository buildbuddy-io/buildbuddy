load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["execution_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_execution/execution_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/remote_execution/operation:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@go_googleapis//google/rpc:status_go_proto",
        "@io_bazel_rules_go//proto/wkt:duration_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
    ],
)

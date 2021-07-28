load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "mount_casfs_lib",
    srcs = ["mount_casfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/mount_casfs",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/casfs",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "mount_casfs",
    embed = [":mount_casfs_lib"],
    visibility = ["//visibility:public"],
)

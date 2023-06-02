load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "vmstart",
    embed = [":vmstart_lib"],
)

go_library(
    name = "vmstart_lib",
    srcs = ["vmstart.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/vmstart",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/firecracker",
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:vmvfs_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)

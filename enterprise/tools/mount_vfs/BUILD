load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "mount_vfs_lib",
    srcs = ["mount_vfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/mount_vfs",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/vfs",
        "//enterprise/server/util/vfs_server",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "mount_vfs",
    embed = [":mount_vfs_lib"],
)

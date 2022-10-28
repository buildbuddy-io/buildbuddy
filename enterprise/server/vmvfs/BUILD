load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vmvfs_lib",
    srcs = ["vmvfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmvfs",
    deps = [
        "//enterprise/server/remote_execution/vfs",
        "//enterprise/server/util/vsock",
        "//proto:vfs_go_proto",
        "//proto:vmvfs_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "vmvfs",
    embed = [":vmvfs_lib"],
    pure = "on",
    static = "on",
)

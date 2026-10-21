load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vmvfs_lib",
    srcs = ["vmvfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmvfs",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/vfs",
        "//enterprise/server/util/vfs_server",
        "//enterprise/server/util/vfscommon",
        "//enterprise/server/util/vsock",
        "//proto:remote_execution_go_proto",
        "//proto:vfs_go_proto",
        "//proto:vmvfs_go_proto",
        "//server/cache/dirtools",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_mdlayher_vsock//:vsock",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

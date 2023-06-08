load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nbdclient",
    srcs = ["nbdclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdclient",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/util/vsock",
        "//proto:nbd_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@com_github_merovius_nbd//:nbd",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

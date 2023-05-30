load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nbdclient",
    srcs = ["nbdclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdclient",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/vsock",
        "//proto:nbd_go_proto",
        "//server/util/canary",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@com_github_samalba_buse_go//buse",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

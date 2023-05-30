load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nbdserver",
    srcs = ["nbdserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdserver",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/nbd/nbdio",
        "//proto:nbd_go_proto",
        "//server/environment",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

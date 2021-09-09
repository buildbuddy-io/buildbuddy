load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vmexec",
    srcs = ["vmexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/casfs",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/dirtools",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@com_github_vishvananda_netlink//:netlink",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_x_sys//unix",
    ],
)

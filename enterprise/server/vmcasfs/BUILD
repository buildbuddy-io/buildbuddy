load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "vmcasfs_lib",
    srcs = ["vmcasfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/vmcasfs",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/casfs",
        "//enterprise/server/util/vsock",
        "//proto:vfs_go_proto",
        "//proto:vmcasfs_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "vmcasfs",
    embed = [":vmcasfs_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vfs",
    srcs = ["vfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:vfs_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@com_github_hanwen_go_fuse_v2//fs",
        "@com_github_hanwen_go_fuse_v2//fuse",
        "@org_golang_google_grpc//status",
    ],
)

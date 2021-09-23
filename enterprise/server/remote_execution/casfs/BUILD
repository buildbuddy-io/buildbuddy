load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "casfs",
    srcs = ["casfs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/casfs",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//proto:vfs_go_proto",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@com_github_hanwen_go_fuse_v2//fs",
        "@com_github_hanwen_go_fuse_v2//fuse",
        "@org_golang_google_grpc//status",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vfs_server",
    srcs = ["vfs_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//proto:vfs_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/status",
        "@com_github_hanwen_go_fuse_v2//fs",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "authutil",
    srcs = ["authutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/authutil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:context_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)

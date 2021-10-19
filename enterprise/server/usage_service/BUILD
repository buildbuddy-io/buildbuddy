load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "usage_service",
    srcs = ["usage_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:usage_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/perms",
    ],
)

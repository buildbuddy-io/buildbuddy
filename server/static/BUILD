load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["static.go"],
    data = [
        "//:VERSION",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/static",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:config_go_proto",
        "//server/environment:go_default_library",
        "//server/version:go_default_library",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

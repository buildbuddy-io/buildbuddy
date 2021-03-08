load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/github",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/workflow:go_default_library",
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/random:go_default_library",
        "//server/util/status:go_default_library",
    ],
)

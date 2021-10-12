load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/github",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
    ],
)

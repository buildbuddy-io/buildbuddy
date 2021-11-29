load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "authutil",
    srcs = ["authutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/authutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/role",
        "//server/util/status",
    ],
)

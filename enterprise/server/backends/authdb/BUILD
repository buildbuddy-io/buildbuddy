load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "authdb",
    srcs = ["authdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/status",
    ],
)

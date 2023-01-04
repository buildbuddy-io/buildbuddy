load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ziputil",
    srcs = ["ziputil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/ziputil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:zip_go_proto",
        "//server/util/status",
    ],
)

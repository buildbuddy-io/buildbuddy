load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "filter",
    srcs = ["filter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/filter",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:stat_filter_go_proto",
        "//server/util/status",
    ],
)

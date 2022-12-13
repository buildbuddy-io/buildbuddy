load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "besutil",
    srcs = ["besutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/besutil",
    visibility = ["//visibility:public"],
    deps = ["//proto:build_event_stream_go_proto"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "logstream",
    srcs = ["logstream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/logstream",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:logstream_go_proto",
        "//server/backends/chunkstore",
        "//server/environment",
        "//server/util/keyval",
        "//server/util/status",
    ],
)


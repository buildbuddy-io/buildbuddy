load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "evictor",
    srcs = ["evictor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/evictor",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/usagegossiper",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/util/log",
    ],
)

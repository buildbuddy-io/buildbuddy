load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "filestore",
    srcs = ["filestore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:raft_go_proto",
        "//server/util/disk",
        "//server/util/status",
    ],
)

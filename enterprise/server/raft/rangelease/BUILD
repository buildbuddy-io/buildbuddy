load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rangelease",
    srcs = ["rangelease.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/nodeliveness",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)

go_test(
    name = "rangelease_test",
    srcs = ["rangelease_test.go"],
    deps = [
        ":rangelease",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/nodeliveness",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//status",
    ],
)

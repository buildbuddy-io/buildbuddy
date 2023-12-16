load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "proto",
    srcs = ["proto.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/proto",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_protobuf//proto"],
)

go_test(
    name = "proto_test",
    srcs = ["proto_test.go"],
    deps = [
        ":proto",
        "//proto:cache_go_proto",
        "//proto:distributed_cache_go_proto",
        "//proto:raft_go_proto",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//require",
    ],
)

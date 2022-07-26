load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "registry",
    srcs = ["registry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/util/log",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v3//config",
        "@com_github_lni_dragonboat_v3//raftio",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "registry_test",
    size = "small",
    srcs = ["registry_test.go"],
    deps = [
        ":registry",
        "//server/gossip",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
    ],
)

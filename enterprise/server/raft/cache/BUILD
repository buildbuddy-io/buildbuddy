load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cache",
    srcs = ["cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/cache",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/api",
        "//enterprise/server/raft/bringup",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/gossip",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/statemachine",
        "//proto:raft_service_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/util/grpc_server",
        "//server/util/log",
        "//server/util/network",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//config",
        "@com_github_lni_dragonboat_v3//raftio",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//reflection",
    ],
)

go_test(
    name = "cache_test",
    srcs = ["cache_test.go"],
    deps = [
        ":cache",
        "//server/testutil/app",
        "//server/testutil/testenv",
        "//server/util/disk",
        "@com_github_stretchr_testify//require",
    ],
)

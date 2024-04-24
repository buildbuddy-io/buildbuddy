load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "txn",
    srcs = ["txn.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "txn_test",
    srcs = ["txn_test.go"],
    deps = [
        ":txn",
        "//enterprise/server/raft/bringup",
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/listener",
        "//enterprise/server/raft/logger",
        "//enterprise/server/raft/rangecache",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/sender",
        "//enterprise/server/raft/store",
        "//enterprise/server/util/pebble",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/disk",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_lni_dragonboat_v4//config",
        "@com_github_lni_dragonboat_v4//raftio",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "testutil",
    testonly = 1,
    srcs = ["testutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/bringup",
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/listener",
        "//enterprise/server/raft/logger",
        "//enterprise/server/raft/rangecache",
        "//enterprise/server/raft/registry",
        "//enterprise/server/raft/replica",
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
        "@com_github_lni_dragonboat_v4//client",
        "@com_github_lni_dragonboat_v4//config",
        "@com_github_lni_dragonboat_v4//raftio",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_lni_goutils//random",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "testutil_test",
    srcs = ["testutil_test.go"],
    deps = [":testutil"],
)

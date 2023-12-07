load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rangelease",
    srcs = ["rangelease.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease",
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/nodeliveness",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/replica",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "rangelease_test",
    size = "small",
    srcs = ["rangelease_test.go"],
    deps = [
        ":rangelease",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/nodeliveness",
        "//enterprise/server/raft/replica",
        "//enterprise/server/raft/sender",
        "//enterprise/server/util/pebble",
        "//proto:raft_go_proto",
        "//server/testutil/testfs",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_lni_dragonboat_v4//client",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_lni_goutils//random",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)

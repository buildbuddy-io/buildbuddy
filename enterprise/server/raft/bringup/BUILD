load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "bringup",
    srcs = ["bringup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup",
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/config",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/util/log",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

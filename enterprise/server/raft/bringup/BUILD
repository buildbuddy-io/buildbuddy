load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "bringup",
    srcs = ["bringup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup",
    deps = [
        "//enterprise/server/filestore",
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/header",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@org_golang_x_sync//errgroup",
    ],
)

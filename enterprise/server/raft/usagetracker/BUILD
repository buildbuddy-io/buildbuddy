load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "usagetracker",
    srcs = ["usagetracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//server/gossip",
        "//server/metrics",
        "//server/util/approxlru",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@com_github_hashicorp_serf//serf",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

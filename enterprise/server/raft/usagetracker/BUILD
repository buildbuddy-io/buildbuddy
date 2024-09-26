load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "usagetracker",
    srcs = ["usagetracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/usagetracker",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/filestore",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//enterprise/server/util/pebble",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/approxlru",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/timeutil",
        "@com_github_docker_go_units//:go-units",
        "@com_github_hashicorp_serf//serf",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

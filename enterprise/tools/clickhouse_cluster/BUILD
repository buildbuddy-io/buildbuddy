load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "clickhouse_cluster_lib",
    srcs = ["clickhouse_cluster.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouse_cluster",
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "@com_github_containerd_errdefs//:errdefs",
        "@com_github_docker_go_units//:go-units",
        "@com_github_moby_moby_api//pkg/stdcopy",
        "@com_github_moby_moby_api//types/container",
        "@com_github_moby_moby_api//types/network",
        "@com_github_moby_moby_client//:client",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "clickhouse_cluster",
    embed = [":clickhouse_cluster_lib"],
)

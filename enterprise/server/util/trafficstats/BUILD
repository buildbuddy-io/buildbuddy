load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "trafficstats",
    srcs = ["trafficstats.go"],
    embedsrcs = [
        "data/aws.csv",
        "data/azure.csv",
        "data/gcp.csv",
        "data/github.csv",
        "data/macstadium.csv",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/trafficstats",
    deps = [
        "//server/metrics",
        "//server/util/alert",
        "//server/util/claims",
        "//server/util/clientip",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//peer",
        "@org_golang_google_grpc//stats",
    ],
)

go_test(
    name = "trafficstats_test",
    srcs = ["trafficstats_test.go"],
    embed = [":trafficstats"],
    deps = [
        "//server/metrics",
        "//server/testutil/testmetrics",
        "//server/util/claims",
        "//server/util/clientip",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//peer",
        "@org_golang_google_grpc//stats",
    ],
)

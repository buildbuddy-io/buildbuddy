load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "egress",
    srcs = ["egress.go"],
    embedsrcs = [
        "//enterprise/server/util/egress/aws:aws.csv",
        "//enterprise/server/util/egress/gcp:gcp.csv",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress",
    deps = [
        "//server/metrics",
        "//server/util/claims",
        "//server/util/status",
        "@org_golang_google_grpc//stats",
    ],
)

go_test(
    name = "egress_test",
    srcs = ["egress_test.go"],
    embed = [":egress"],
    deps = [
        "//server/metrics",
        "//server/testutil/testmetrics",
        "//server/util/claims",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//stats",
    ],
)

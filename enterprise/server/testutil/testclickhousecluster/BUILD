load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testclickhousecluster",
    testonly = 1,
    srcs = ["testclickhousecluster.go"],
    data = ["//enterprise/tools/clickhouse_cluster"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testclickhousecluster",
    x_defs = {
        "clickhouseClusterRlocationpath": "$(rlocationpath //enterprise/tools/clickhouse_cluster:clickhouse_cluster)",
    },
    deps = [
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

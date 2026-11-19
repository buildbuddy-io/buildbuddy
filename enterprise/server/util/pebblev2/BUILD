load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pebblev2",
    srcs = ["pebblev2.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebblev2",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/alert",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_cockroachdb_pebble_v2//:pebble",
        "@com_github_cockroachdb_pebble_v2//batchrepr",
        "@com_github_cockroachdb_pebble_v2//bloom",
        "@com_github_cockroachdb_pebble_v2//vfs",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "pebblev2_test",
    srcs = ["pebblev2_test.go"],
    deps = [
        ":pebblev2",
        "//server/testutil/testfs",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

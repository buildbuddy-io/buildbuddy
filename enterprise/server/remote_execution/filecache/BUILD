load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filecache",
    srcs = ["filecache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/claims",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/random",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "filecache_test",
    size = "small",
    srcs = ["filecache_test.go"],
    deps = [
        ":filecache",
        "//proto:remote_execution_go_proto",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/testutil/testfs",
        "//server/testutil/testmetrics",
        "//server/util/claims",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

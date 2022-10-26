load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//tools/vmstart:__pkg__",
])

go_library(
    name = "filecache",
    srcs = ["filecache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "//server/util/uuid",
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
        "//server/testutil/testfs",
        "//server/util/hash",
        "@com_github_stretchr_testify//assert",
    ],
)

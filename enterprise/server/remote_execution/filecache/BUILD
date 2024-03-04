load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filecache",
    srcs = [
        "filecache.go",
        "filecache_proxy.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache",
    deps = [
        "//enterprise/server/util/cacheproxy",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/claims",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/random",
        "//server/util/status",
        "@com_github_bits_and_blooms_bloom_v3//:bloom",
        "@com_github_hashicorp_serf//serf",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_x_sync//errgroup",
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

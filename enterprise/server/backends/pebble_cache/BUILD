load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "pebble_cache",
    srcs = ["pebble_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/filestore",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/cache/config",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "//server/util/flagutil/types",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_cockroachdb_pebble//:pebble",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "pebble_cache_test",
    srcs = ["pebble_cache_test.go"],
    deps = [
        ":pebble_cache",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
    ],
)

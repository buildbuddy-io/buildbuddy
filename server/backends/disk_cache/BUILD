load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "disk_cache",
    srcs = ["disk_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/cache/config",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/alert",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/statusz",
        "@com_github_docker_go_units//:go-units",
        "@com_github_elastic_gosigar//:gosigar",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_text//language",
        "@org_golang_x_text//message",
    ],
)

go_test(
    name = "disk_cache_test",
    size = "medium",
    srcs = ["disk_cache_test.go"],
    deps = [
        ":disk_cache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

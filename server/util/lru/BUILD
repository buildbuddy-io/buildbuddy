load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "lru",
    srcs = ["lru.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/lru",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "lru_test",
    size = "small",
    srcs = ["lru_test.go"],
    deps = [
        ":lru",
        "//server/metrics",
        "//server/testutil/testmetrics",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_stretchr_testify//require",
    ],
)

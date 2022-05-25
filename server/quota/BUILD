load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "quota",
    srcs = ["rate_limiter_pool.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/quota",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:quota_config_go_proto",
        "//server/environment",
        "//server/quota/config",
        "//server/util/status",
        "@com_github_throttled_throttled_v2//:throttled",
        "@com_github_throttled_throttled_v2//store/goredisstore.v8:goredisstore_v8",
    ],
)

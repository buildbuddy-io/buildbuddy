load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redisutil",
    srcs = ["redisutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil",
    deps = [
        "//enterprise/server/experiments",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_cespare_xxhash_v2//:xxhash",
        "@com_github_dgryski_go_rendezvous//:go-rendezvous",
        "@com_github_go_redis_redis_extra_redisotel_v8//:redisotel",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "redisutil_test",
    size = "small",
    srcs = ["redisutil_test.go"],
    deps = [
        ":redisutil",
        "//enterprise/server/experiments",
        "//enterprise/server/testutil/testredis",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/log",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_cmp//cmp/cmpopts",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

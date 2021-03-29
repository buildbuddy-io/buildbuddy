load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["redisutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//server/interfaces:go_default_library",
        "@com_github_go_redis_redis_v8//:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["redisutil_test.go"],
    embed = [":go_default_library"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "@com_github_go_redis_redis_v8//:go_default_library",
        "@com_github_google_go_cmp//cmp:go_default_library",
        "@com_github_google_go_cmp//cmp/cmpopts:go_default_library",
    ],
)

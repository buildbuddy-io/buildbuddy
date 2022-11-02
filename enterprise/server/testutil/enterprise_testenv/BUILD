load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "enterprise_testenv",
    testonly = 1,
    srcs = ["enterprise_testenv.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv",
    deps = [
        "//enterprise/server/backends/authdb",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/backends/userdb",
        "//enterprise/server/util/redisutil",
        "//server/testutil/testenv",
        "//server/testutil/testhealthcheck",
        "//server/util/log",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
    ],
)

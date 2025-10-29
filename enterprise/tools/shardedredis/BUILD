load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "shardedredis_lib",
    srcs = ["shardedredis.go"],
    data = [
        ":redis.conf",
        "@redis//:redis-server",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/shardedredis",
    visibility = ["//visibility:private"],
    x_defs = {
        "redisRlocationpath": "$(rlocationpath @redis//:redis-server)",
        "configRlocationpath": "$(rlocationpath :redis.conf)",
    },
    deps = [
        "//server/util/log",
        "//server/util/shlex",
        "@com_github_armon_circbuf//:circbuf",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

go_binary(
    name = "shardedredis",
    embed = [":shardedredis_lib"],
    visibility = ["//visibility:public"],
)

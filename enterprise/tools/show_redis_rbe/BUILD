load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "show_redis_rbe_lib",
    srcs = ["show_redis_rbe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/show_redis_rbe",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/redisutil",
        "//proto:remote_execution_go_proto",
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_exp//maps",
    ],
)

go_binary(
    name = "show_redis_rbe",
    embed = [":show_redis_rbe_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

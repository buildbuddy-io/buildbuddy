load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "dsingleflight",
    srcs = ["dsingleflight.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/dsingleflight",
    deps = [
        "//proto:grpc_status_go_proto",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "dsingleflight_test",
    size = "small",
    srcs = ["dsingleflight_test.go"],
    embed = [":dsingleflight"],
    deps = [
        "//enterprise/server/testutil/testredis",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "dsingleflight",
    srcs = ["dsingleflight.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/dsingleflight",
    deps = [
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
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

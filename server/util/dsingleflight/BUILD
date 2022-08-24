load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "dsingleflight",
    srcs = ["dsingleflight.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/dsingleflight",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@go_googleapis//google/rpc:status_go_proto",
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

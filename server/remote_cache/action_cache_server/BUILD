load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "action_cache_server",
    srcs = ["action_cache_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/remote_cache/namespace",
        "//server/util/capabilities",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_google_uuid//:uuid",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "action_cache_server_test",
    srcs = ["action_cache_server_test.go"],
    deps = [
        ":action_cache_server",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

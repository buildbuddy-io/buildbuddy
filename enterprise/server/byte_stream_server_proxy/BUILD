load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "byte_stream_server_proxy",
    srcs = ["byte_stream_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "byte_stream_server_proxy_test",
    size = "small",
    srcs = ["byte_stream_server_proxy_test.go"],
    embed = [":byte_stream_server_proxy"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/digest",
        "//server/testutil/byte_stream",
        "//server/testutil/testcompression",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/compression",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//status",
    ],
)

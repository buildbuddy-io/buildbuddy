load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "byte_stream_server_proxy",
    srcs = ["byte_stream_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/proxy_util",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "byte_stream_server_proxy_test",
    size = "medium",
    srcs = ["byte_stream_server_proxy_test.go"],
    embed = [":byte_stream_server_proxy"],
    deps = [
        "//enterprise/server/atime_updater",
        "//enterprise/server/util/proxy_util",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/digest",
        "//server/testutil/byte_stream",
        "//server/testutil/testcompression",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/compression",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "bytestream_test",
    timeout = "long",
    srcs = ["bytestream_server_benchmark_test.go"],
    args = [
        "-test.bench=.",
    ],
    tags = ["performance"],
    deps = [
        "//enterprise/server/backends/distributed",
        "//enterprise/server/backends/pebble_cache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/config",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/compression",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

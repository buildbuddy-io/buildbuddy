load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "content_addressable_storage_server_proxy",
    srcs = ["content_addressable_storage_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/rpcutil",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//codes",
    ],
)

go_test(
    name = "content_addressable_storage_server_proxy_test",
    size = "small",
    srcs = ["content_addressable_storage_server_proxy_test.go"],
    embed = [":content_addressable_storage_server_proxy"],
    deps = [
        "//enterprise/server/atime_updater",
        "//enterprise/server/byte_stream_server_proxy",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/testutil/cas",
        "//server/testutil/testenv",
        "//server/util/uuid",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
    ],
)

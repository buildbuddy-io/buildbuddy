load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocifetcher_server_proxy",
    srcs = ["ocifetcher_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ocifetcher_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/ocicache",
        "//proto:oci_fetcher_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/compression",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

go_test(
    name = "ocifetcher_server_proxy_test",
    size = "medium",
    srcs = ["ocifetcher_server_proxy_test.go"],
    embed = [":ocifetcher_server_proxy"],
    deps = [
        "//enterprise/server/oci/ocifetcher",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testregistry",
        "//proto:capability_go_proto",
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/util/claims",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

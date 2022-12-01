load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cacheproxy",
    srcs = ["cacheproxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy",
    deps = [
        "//proto:distributed_cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/resources",
        "//server/util/bytebufferpool",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//connectivity",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//reflection",
    ],
)

go_test(
    name = "cacheproxy_test",
    size = "small",
    srcs = ["cacheproxy_test.go"],
    deps = [
        ":cacheproxy",
        "//proto:distributed_cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testcompression",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/compression",
        "//server/util/prefix",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)

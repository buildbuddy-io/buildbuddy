load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "action_cache_server_test",
    srcs = ["action_cache_server_test.go"],
    gotags = ["test"],
    deps = [
        "//enterprise/server/clientidentity",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/capabilities_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//test/bufconn",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

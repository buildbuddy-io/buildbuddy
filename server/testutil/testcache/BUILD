load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testcache",
    testonly = 1,
    srcs = ["testcache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testcache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

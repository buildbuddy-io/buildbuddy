load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "chunk_metadata_store",
    srcs = ["chunk_metadata_store.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/chunk_metadata_store",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/proto",
        "//server/util/status",
    ],
)

go_test(
    name = "chunk_metadata_store_test",
    srcs = ["chunk_metadata_store_test.go"],
    data = ["//website:static_files"],
    embed = [":chunk_metadata_store"],
    deps = [
        "//enterprise/server/util/chunker",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

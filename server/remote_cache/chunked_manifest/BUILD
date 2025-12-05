load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "chunked_manifest",
    srcs = ["chunked_manifest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "chunked_manifest_test",
    srcs = ["chunked_manifest_test.go"],
    deps = [
        ":chunked_manifest",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

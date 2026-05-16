load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "capabilities_server",
    srcs = ["capabilities_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:capability_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:semver_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/chunking",
        "//server/remote_cache/config",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
    ],
)

go_test(
    name = "capabilities_server_test",
    srcs = ["capabilities_server_test.go"],
    embed = [":capabilities_server"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)

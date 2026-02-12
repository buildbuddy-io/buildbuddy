load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cache_server",
    srcs = ["cache_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cache_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:cache_go_proto",
        "//proto:cache_service_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/prefix",
        "//server/util/status",
    ],
)

go_test(
    name = "cache_server_test",
    srcs = ["cache_server_test.go"],
    deps = [
        ":cache_server",
        "//proto:cache_go_proto",
        "//server/testutil/testdigest",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
    ],
)

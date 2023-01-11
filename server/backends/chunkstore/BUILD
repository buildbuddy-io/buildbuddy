load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "chunkstore",
    srcs = ["chunkstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/background",
        "//server/util/status",
        "//server/util/timeutil",
    ],
)

go_test(
    name = "chunkstore_test",
    size = "small",
    srcs = ["chunkstore_test.go"],
    embed = [":chunkstore"],
    deps = [
        "//server/testutil/mockstore",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
    ],
)

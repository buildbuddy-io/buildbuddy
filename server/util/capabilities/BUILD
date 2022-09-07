load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "capabilities",
    srcs = ["capabilities.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/capabilities",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/util/perms",
        "//server/util/status",
    ],
)

go_test(
    name = "capabilities_test",
    size = "small",
    srcs = ["capabilities_test.go"],
    deps = [
        ":capabilities",
        "//proto:api_key_go_proto",
        "//server/interfaces",
        "//server/nullauth",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
    ],
)

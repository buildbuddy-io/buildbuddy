load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["capabilities.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/capabilities",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["capabilities_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:api_key_go_proto",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

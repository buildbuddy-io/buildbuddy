load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "identity",
    srcs = ["identity.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_buddy_go_proto",
        "//server/util/git",
        "//server/util/status",
        "@bazel_gazelle//label",
    ],
)

go_test(
    name = "identity_test",
    srcs = ["identity_test.go"],
    deps = [
        ":identity",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

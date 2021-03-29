load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "lockingbuffer",
    srcs = ["lockingbuffer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["lockingbuffer_test.go"],
    # keep
    embed = [],
    deps = [
        ":go_default_library",  # keep
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

go_test(
    name = "lockingbuffer_test",
    srcs = ["lockingbuffer_test.go"],
    deps = [
        ":lockingbuffer",
        "@com_github_stretchr_testify//assert",
    ],
)

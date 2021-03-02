load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["locking_buffer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/locking_buffer",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["locking_buffer_test.go"],
    deps = [
        ":go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

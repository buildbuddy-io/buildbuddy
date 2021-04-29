load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "test_data",
    srcs = ["test_data.go"],
    embedsrcs = [
        "basic.yaml",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config/test_data",
    visibility = [
        "//enterprise/server/workflow/config:__subpackages__",
    ],
)

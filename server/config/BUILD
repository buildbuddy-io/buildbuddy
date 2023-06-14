load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/config",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/flagutil",
        "//server/util/flagutil/common",
        "//server/util/flagutil/types",
        "//server/util/flagutil/yaml",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    deps = [
        ":config",
        "//server/util/flagutil",
        "//server/util/flagutil/common",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)

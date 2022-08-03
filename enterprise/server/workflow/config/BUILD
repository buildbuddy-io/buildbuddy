load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/webhooks/webhook_data",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

go_test(
    name = "config_test",
    size = "small",
    srcs = ["config_test.go"],
    deps = [
        ":config",
        "//enterprise/server/workflow/config/test_data",
        "@com_github_stretchr_testify//assert",
    ],
)

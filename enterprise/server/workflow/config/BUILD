load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config",
    deps = [
        "//enterprise/server/webhooks/webhook_data",
        "//proto:runner_go_proto",
        "//server/build_event_protocol/accumulator",
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
        "//proto:runner_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_config",
    srcs = ["cache_config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/cache_config",
    deps = [
        "//server/util/disk",
    ],
)

go_test(
    name = "cache_config_test",
    srcs = ["cache_config_test.go"],
    deps = [
        ":cache_config",
        "//server/util/flag",
        "//server/util/flagutil/yaml",
        "//server/util/redact",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "metronome",
    srcs = ["metronome.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome",
    deps = [
        "//server/http/httpclient",
        "//server/usage/sku",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/region",
        "//server/util/status",
    ],
)

go_test(
    name = "metronome_test",
    size = "small",
    srcs = ["metronome_test.go"],
    embed = [":metronome"],
    deps = [
        "//server/usage/sku",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

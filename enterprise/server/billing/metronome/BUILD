load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "metronome",
    srcs = ["metronome.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome",
    visibility = ["//enterprise/server/usage:__pkg__"],
    deps = [
        "//server/http/httpclient",
        "//server/tables",
        "//server/usage/sku",
        "//server/util/flag",
        "//server/util/region",
        "//server/util/status",
        "//server/util/usageutil",
    ],
)

go_test(
    name = "metronome_test",
    size = "small",
    srcs = ["metronome_test.go"],
    embed = [":metronome"],
    deps = [
        "//server/tables",
        "//server/usage/sku",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

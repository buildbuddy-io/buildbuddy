load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "relay",
    srcs = ["relay.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/relay",
    deps = [
        "//enterprise/gateway/server",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/relaywire",
        "//server/util/status",
    ],
)

go_test(
    name = "relay_test",
    srcs = [
        "relay_e2e_test.go",
        "relay_test.go",
    ],
    embed = [":relay"],
    deps = [
        "//enterprise/gateway/apikeyauth",
        "//enterprise/gateway/certauth",
        "//enterprise/gateway/server",
        "//enterprise/gateway/testgateway",
        "//server/testutil/testauth",
        "//server/testutil/testauthrelay",
        "//server/util/relaywire",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

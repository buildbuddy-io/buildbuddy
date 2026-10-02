load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "stripe",
    srcs = ["stripe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//server/tables",
        "//server/util/flag",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "stripe_test",
    srcs = ["stripe_test.go"],
    deps = [
        ":stripe",
        "//server/tables",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

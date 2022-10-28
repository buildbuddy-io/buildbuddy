load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "bitbucket",
    srcs = ["bitbucket.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket",
    deps = [
        "//enterprise/server/util/fieldgetter",
        "//enterprise/server/webhooks/webhook_data",
        "//server/interfaces",
        "//server/util/status",
    ],
)

go_test(
    name = "bitbucket_test",
    size = "small",
    srcs = ["bitbucket_test.go"],
    deps = [
        ":bitbucket",
        "//enterprise/server/webhooks/bitbucket/test_data",
        "//server/interfaces",
        "@com_github_stretchr_testify//assert",
    ],
)

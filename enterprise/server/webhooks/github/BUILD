load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github",
    deps = [
        "//enterprise/server/util/fieldgetter",
        "//enterprise/server/webhooks/webhook_data",
        "//server/interfaces",
        "//server/util/git",
        "//server/util/status",
        "@com_github_google_go_github_v43//github",
        "@org_golang_x_oauth2//:oauth2",
    ],
)

go_test(
    name = "github_test",
    size = "small",
    srcs = ["github_test.go"],
    deps = [
        ":github",
        "//enterprise/server/webhooks/github/test_data",
        "//server/interfaces",
        "@com_github_stretchr_testify//assert",
    ],
)

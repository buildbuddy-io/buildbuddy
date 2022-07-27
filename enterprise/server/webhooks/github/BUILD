load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "github",
    srcs = ["github.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
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
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        ":github",
        "//enterprise/server/webhooks/github/test_data",
        "//server/interfaces",
        "@com_github_stretchr_testify//assert",
    ],
)

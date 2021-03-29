load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//extras:embed_data.bzl", "go_embed_data")

go_library(
    name = "bitbucket",
    srcs = ["bitbucket.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/fieldgetter",
        "//enterprise/server/webhooks/webhook_data",
        "//server/util/status",
    ],
)

go_embed_data(
    name = "data",
    testonly = 1,
    srcs = glob(["test_data/**/*"]),
    package = "bitbucket_test",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["bitbucket_test.go"],
    # keep
    embed = [":data"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        ":go_default_library",  # keep
        "//enterprise/server/webhooks/webhook_data:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

go_test(
    name = "bitbucket_test",
    srcs = ["bitbucket_test.go"],
    deps = [
        ":bitbucket",
        "//enterprise/server/webhooks/webhook_data",
        "@com_github_stretchr_testify//assert",
    ],
)

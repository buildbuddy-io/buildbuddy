load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//extras:embed_data.bzl", "go_embed_data")

go_library(
    name = "go_default_library",
    srcs = ["bitbucket.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/dynamic:go_default_library",
        "//enterprise/server/webhooks/webhook_data:go_default_library",
        "//server/util/status:go_default_library",
    ],
)

go_embed_data(
    name = "data",
    testonly = 1,
    srcs = glob(["test_data/**/*"]),
    package = "bitbucket_test",
)

go_test(
    name = "go_default_test",
    srcs = ["bitbucket_test.go"],
    # keep
    embed = [":data"],
    deps = [
        ":go_default_library",
        "//enterprise/server/webhooks/webhook_data:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

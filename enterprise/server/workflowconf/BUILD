load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//extras:embed_data.bzl", "go_embed_data")

go_library(
    name = "go_default_library",
    srcs = ["workflowconf.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowconf",
    visibility = ["//enterprise:__subpackages__"],
    deps = ["@in_gopkg_yaml_v2//:go_default_library"],
)

go_embed_data(
    name = "test_data",
    srcs = glob(["test_data/**/*"]),
    package = "workflowconf_test",
    visibility = ["//enterprise:__subpackages__"],
)

go_test(
    name = "go_default_test",
    srcs = ["workflowconf_test.go"],
    # keep
    embed = [":test_data"],
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        ":go_default_library",  # keep
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["fieldgetter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter",
    visibility = ["//enterprise:__subpackages__"],
)

go_test(
    name = "go_default_test",
    srcs = ["fieldgetter_test.go"],
    # keep
    embed = [],
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        ":go_default_library",  # keep
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["fieldgetter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter",
    visibility = ["//visibility:public"],
    deps = ["//server/util/status:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["fieldgetter_test.go"],
    deps = [
        ":go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

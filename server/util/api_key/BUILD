load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "api_key",
    srcs = ["api_key.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/api_key",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/tables",
    ],
)

go_test(
    name = "api_key_test",
    srcs = ["api_key_test.go"],
    deps = [
        ":api_key",
        "//proto:api_key_go_proto",
        "//server/tables",
        "@com_github_stretchr_testify//require",
    ],
)

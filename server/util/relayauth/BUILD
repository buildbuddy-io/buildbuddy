load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "relayauth",
    srcs = ["relayauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/relayauth",
    visibility = ["//visibility:public"],
    deps = ["@com_github_golang_jwt_jwt_v4//:jwt"],
)

go_test(
    name = "relayauth_test",
    srcs = ["relayauth_test.go"],
    embed = [":relayauth"],
    deps = [
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

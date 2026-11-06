load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "credentials",
    srcs = ["credentials.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/credentials",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/relayauth",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "credentials_test",
    srcs = ["credentials_test.go"],
    embed = [":credentials"],
    deps = [
        "//server/testutil/testauthrelay",
        "//server/util/relayauth",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)

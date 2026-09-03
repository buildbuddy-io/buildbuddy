load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testauthrelay",
    srcs = ["testauthrelay.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testauthrelay",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/relayauth",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "log",
    srcs = ["log.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/log",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log/gcp",
        "//server/util/status",
        "@com_github_rs_zerolog//:zerolog",
        "@com_github_rs_zerolog//log",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "log_test",
    srcs = ["log_test.go"],
    deps = [
        ":log",
        "//server/util/testing/flags",
        "@com_github_rs_zerolog//:zerolog",
        "@com_github_rs_zerolog//log",
        "@com_github_stretchr_testify//assert",
    ],
)

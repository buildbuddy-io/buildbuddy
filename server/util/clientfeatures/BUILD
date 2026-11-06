load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "clientfeatures",
    srcs = ["clientfeatures.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clientfeatures",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "clientfeatures_test",
    srcs = ["clientfeatures_test.go"],
    deps = [
        ":clientfeatures",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)

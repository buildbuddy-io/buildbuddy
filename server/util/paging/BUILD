load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "paging",
    srcs = ["paging.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/paging",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:pagination_go_proto",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "paging_test",
    size = "small",
    srcs = ["paging_test.go"],
    deps = [
        ":paging",
        "//proto:pagination_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "capabilities_filter",
    srcs = ["capabilities_filter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/capabilities_filter",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/util/capabilities",
        "//server/util/status",
    ],
)

go_test(
    name = "capabilities_filter_test",
    size = "small",
    srcs = ["capabilities_filter_test.go"],
    deps = [
        ":capabilities_filter",
        "//proto:api_key_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
    ],
)

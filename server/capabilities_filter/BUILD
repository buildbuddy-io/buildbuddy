load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "capabilities_filter",
    srcs = ["capabilities_filter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/capabilities_filter",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/authutil",
        "//server/util/perms",
        "//server/util/role",
        "//server/util/status",
    ],
)

go_test(
    name = "capabilities_filter_test",
    size = "small",
    srcs = ["capabilities_filter_test.go"],
    deps = [
        ":capabilities_filter",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "@com_github_stretchr_testify//assert",
    ],
)

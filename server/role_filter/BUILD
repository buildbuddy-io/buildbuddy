load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "role_filter",
    srcs = ["role_filter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/role_filter",
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
    name = "role_filter_test",
    size = "small",
    srcs = ["role_filter_test.go"],
    deps = [
        ":role_filter",
        "//proto:buildbuddy_service_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "@com_github_stretchr_testify//assert",
    ],
)

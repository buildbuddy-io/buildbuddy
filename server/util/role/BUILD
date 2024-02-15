load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "role",
    srcs = ["role.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/role",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//server/util/status",
    ],
)

go_test(
    name = "role_test",
    srcs = ["role_test.go"],
    deps = [
        ":role",
        "//proto:group_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)

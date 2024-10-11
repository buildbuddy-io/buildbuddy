load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "buildbuddy_server_test",
    srcs = ["buildbuddy_server_test.go"],
    deps = [
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//server/buildbuddy_server",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

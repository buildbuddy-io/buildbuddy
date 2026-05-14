load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "buildbuddy_server_test",
    srcs = ["buildbuddy_server_test.go"],
    deps = [
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/buildbuddy_server",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/role",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

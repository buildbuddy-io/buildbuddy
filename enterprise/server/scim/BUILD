load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "scim",
    srcs = ["scim.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scim",
    deps = [
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/log",
        "//server/util/role",
        "//server/util/status",
    ],
)

go_test(
    name = "scim_test",
    size = "small",
    srcs = ["scim_test.go"],
    deps = [
        ":scim",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testhttp",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)

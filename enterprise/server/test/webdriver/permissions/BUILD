load("//rules/webdriver:index.bzl", "go_web_test_suite")

package(default_visibility = ["//enterprise:__subpackages__"])

go_web_test_suite(
    name = "permissions_test",
    srcs = ["permissions_test.go"],
    shard_count = 3,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/permissionstest",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:user_id_go_proto",
        "//server/tables",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "//server/util/perms",
        "@com_github_stretchr_testify//require",
    ],
)

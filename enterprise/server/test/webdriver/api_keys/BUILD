load("//rules/webdriver:index.bzl", "go_web_test_suite")

package(default_visibility = ["//enterprise:__subpackages__"])

go_web_test_suite(
    name = "api_keys_test",
    srcs = ["api_keys_test.go"],
    shard_count = 4,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//server/testutil/app",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//require",
    ],
)

load("//rules/webdriver:index.bzl", "go_web_test_suite")

go_web_test_suite(
    name = "shortcuts_test",
    size = "small",
    srcs = ["shortcuts_test.go"],
    shard_count = 2,
    tags = ["manual"],  # TODO(iain-macdonald): Fix and re-enable
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//require",
        "@com_github_tebeka_selenium//:selenium",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

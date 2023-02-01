load("//rules/webdriver:index.bzl", "go_web_test_suite")

go_web_test_suite(
    name = "invocation_test",
    size = "small",
    srcs = ["invocation_test.go"],
    shard_count = 1,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

load("//rules/webdriver:index.bzl", "go_web_test_suite")

go_web_test_suite(
    name = "setup_test",
    size = "small",
    srcs = ["setup_test.go"],
    shard_count = 2,
    deps = [
        "//server/testutil/buildbuddy",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//assert",
    ],
)

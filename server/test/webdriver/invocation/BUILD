load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

bazel_versions_test(
    name = "invocation_test",
    size = "small",
    srcs = ["invocation_test.go"],
    rule = go_web_test_suite,
    shard_count = 2,
    deps = [
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

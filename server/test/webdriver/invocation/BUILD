load("@io_bazel_rules_webtesting//web:go.bzl", "go_web_test_suite")

go_web_test_suite(
    name = "invocation_test",
    srcs = ["invocation_test.go"],
    browsers = [
        "@io_bazel_rules_webtesting//browsers:chromium-local",
    ],
    shard_count = 2,
    tags = [
        "browser",
        "manual",
        "native",
    ],
    deps = [
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

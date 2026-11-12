load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "shortcuts_test",
    size = "medium",
    srcs = ["shortcuts_test.go"],
    rule = go_web_test_suite,
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

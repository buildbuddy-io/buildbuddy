load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "invocation_test",
    srcs = ["invocation_test.go"],
    exec_properties = {
        # This test can be run against deployed buildbuddy instances, so it
        # requires networking.
        "test.dockerNetwork": "bridge",
    },
    rule = go_web_test_suite,
    shard_count = 3,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

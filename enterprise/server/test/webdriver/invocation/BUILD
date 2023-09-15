load("//rules/webdriver:index.bzl", "go_web_test_suite")

go_web_test_suite(
    name = "invocation_test",
    srcs = ["invocation_test.go"],
    exec_properties = {
        # This test can be run against deployed buildbuddy instances, so it
        # requires networking.
        "test.dockerNetwork": "bridge",
    },
    shard_count = 2,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//server/testutil/testbazel",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

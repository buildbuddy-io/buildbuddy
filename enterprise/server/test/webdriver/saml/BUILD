load("//rules/webdriver:index.bzl", "go_web_test_suite")

package(default_visibility = ["//enterprise:__subpackages__"])

go_web_test_suite(
    name = "saml_test",
    srcs = ["saml_test.go"],
    exec_properties = {
        "test.EstimatedComputeUnits": "3",
        "test.workload-isolation-type": "firecracker",
        "test.recycle-runner": "true",
        "test.init-dockerd": "true",
        "test.runner-recycling-key": "mocksaml",
    },
    shard_count = 1,
    tags = ["docker"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/util/mocksaml",
        "//server/testutil/app",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/testutil/webtester",
        "@com_github_stretchr_testify//require",
    ],
)

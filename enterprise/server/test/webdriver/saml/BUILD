load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "saml_test",
    srcs = ["saml_test.go"],
    exec_properties = {
        "test.EstimatedComputeUnits": "3",
        "test.workload-isolation-type": "firecracker",
        "test.recycle-runner": "true",
        "test.init-dockerd": "true",
        "test.runner-recycling-key": "mocksaml",
    },
    rule = go_web_test_suite,
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

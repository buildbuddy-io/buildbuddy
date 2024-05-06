load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "remote_bazel_test",
    srcs = ["remote_bazel_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "none",
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
        # The tests clone git repos, so make sure they have enough resources to do so
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
    },
    tags = [
        "bare",
    ],
    deps = [
        "//cli/remotebazel",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//proto:api_key_go_proto",
        "//proto:invocation_go_proto",
        "//server/testutil/app",
        "//server/testutil/testport",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

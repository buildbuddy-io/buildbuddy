load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "integration_test",
    srcs = ["remote_bazel_test.go"],
    exec_properties = {
        # Run on bare executor pool because the test spins up firecracker containers
        "test.workload-isolation-type": "none",
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",

        "test.recycle-runner": "true",
        "test.runner-recycling-key": "remote-bazel-integration",

        # The tests clone git repos, so make sure they have enough resources to do so
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
        "include-secrets": "true",
    },
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

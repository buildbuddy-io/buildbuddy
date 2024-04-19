load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "integration_test",
    srcs = ["remote_bazel_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "remote-bazel-integration",
        # The tests clone git repos, so make sure they have enough resources to do so
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
        # Use an image with bazelisk installed
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:271e5e3704d861159c75b8dd6713dbe5a12272ec8ee73d17f89ed7be8026553f",
        "include-secrets": "true",
    },
    deps = [
        "//cli/remotebazel",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

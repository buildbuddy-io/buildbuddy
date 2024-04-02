load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "podman_test",
    size = "large",
    srcs = ["podman_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
        # Use the executor image since it has podman installed.
        "test.container-image": "docker://gcr.io/flame-public/buildbuddy-executor-enterprise:enterprise-v2.47.0",
        # Enable runner recycling to cache test images.
        "test.recycle-runner": "true",
    },
    tags = [
        # TODO(go/b/2945): remove manual tag.
        "manual",
    ],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/remote_execution/containers/podman",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

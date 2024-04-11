load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//:deps.bzl", "PODMAN_VERSION")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "podman_test",
    size = "large",
    srcs = ["podman_test.go"],
    data = ["//enterprise/server/remote_execution/containers/podman:podman-static.tar.gz"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.EstimatedComputeUnits": "4",
        "test.EstimatedFreeDiskBytes": "10GB",
        # Enable runner recycling to cache test images.
        "test.recycle-runner": "true",
        # Include podman version in the snapshot key so that we reinstall from
        # scratch if we change podman versions.
        "test.runner-recycling-key": "podman-static@" + PODMAN_VERSION,
    },
    # TODO: set up build constraints for Firecracker so that this can be skipped
    # locally but not when using --config=remote. For now just use the "docker"
    # tag which we apply to tests that only run on CI.
    tags = [
        "docker",
        # podman is not compatible with linux-sandbox
        "no-sandbox",
    ],
    target_compatible_with = ["@platforms//os:linux"],
    x_defs = {
        "podmanArchiveRlocationpath": "$(rlocationpath //enterprise/server/remote_execution/containers/podman:podman-static.tar.gz)",
    },
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
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)

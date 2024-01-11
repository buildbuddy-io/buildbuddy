load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# To run this test with remote execution, add a test_tag_filter:
# bazel test --config=remote --test_tag_filters=+bare \
# //enterprise/server/test/integration/podman:podman_test
go_test(
    name = "podman_test",
    size = "large",
    srcs = ["podman_test.go"],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
    },
    # Podman tests must run on the bare executors because podman-on-podman /
    # podman-on-firecracker don't work due to issues running overlayfs on
    # overlayfs.
    # TODO(go/b/2944): move to firecracker once
    # firecracker.enable_merged_rootfs works.
    tags = [
        "bare",
        # TODO(go/b/2945): remove manual tag.
        "manual",
    ],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
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

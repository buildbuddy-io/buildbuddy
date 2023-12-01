load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

# To run this test with remote execution, add a test_tag_filter:
# bazel test --config=remote --test_tag_filters=+bare \
# //enterprise/server/remote_execution/containers/podman:podman_test
go_test(
    name = "podman_test",
    srcs = ["podman_test.go"],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
    },
    # TestPodmanRun_RecordsStats is flaky.
    flaky = True,
    tags = [
        "bare",  # Podman tests must be run with bare execution so they aren't nested within another container
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/podman",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

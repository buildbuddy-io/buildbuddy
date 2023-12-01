load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "podman_test",
    size = "large",
    srcs = ["podman_test.go"],
    exec_properties = {
        # Use the executor image since it has podman installed
        # and matches our real executor setup
        "test.container-image": "docker://gcr.io/flame-public/buildbuddy-executor-enterprise:latest",

        # This test pulls some podman images, which can be slow.
        # Use runner recycling to preserve the podman image cache across runs.
        "test.recycle-runner": "true",
    },
    flaky = True,
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

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "linux-sandbox_hostplatform",
    # TODO: arm64 support
    srcs = ["@linux-sandbox_linux-x86_64//file:downloaded"],
    outs = ["linux-sandbox"],
    cmd_bash = "cp $(SRCS) $@",
    target_compatible_with = ["@platforms//os:linux"],
)

go_library(
    name = "linux_sandbox",
    srcs = ["linux_sandbox.go"],
    embedsrcs = ["linux-sandbox"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox",
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/linux_sandbox/proto:linux_sandbox_go_proto",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/ociconv",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/disk",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)

go_test(
    name = "linux_sandbox_test",
    srcs = ["linux_sandbox_test.go"],
    exec_properties = {
        # Use runner recycling on RBE for persistent containerfs storage.
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "linux_sandbox_test",
        # Use a VM on RBE so that we have `mount` permissions needed for overlayfs.
        "test.workload-isolation-type": "firecracker",
    },
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        ":linux_sandbox",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//enterprise/server/util/ociconv",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/testutil/testns",
        "//server/util/nsutil",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

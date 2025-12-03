load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "docker_rbe_test",
    srcs = ["docker_rbe_test.go"],
    data = [
        "@busybox",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "busybox",
    },
    tags = ["docker"],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    x_defs = {
        "busyboxImageRlocationpath": "$(rlocationpath @busybox)",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//enterprise/server/testutil/testregistry",
        "//server/util/disk",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

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
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622",
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
        "//enterprise/server/testutil/testregistry",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

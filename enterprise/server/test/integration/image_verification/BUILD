load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "image_verification_test",
    srcs = ["image_verification_test.go"],
    data = [
        "//enterprise/server/cmd/executor:executor_image",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
    },
    x_defs = {
        "executorImageRlocationpath": "$(rlocationpath //enterprise/server/cmd/executor:executor_image)",
    },
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/testutil/testregistry",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_docker_docker//client",
    ],
)

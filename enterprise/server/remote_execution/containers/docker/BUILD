load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "docker",
    srcs = ["docker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker",
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/executorplatform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/random",
        "//server/util/status",
        "@com_github_containerd_errdefs//:errdefs",
        "@com_github_docker_go_units//:go-units",
        "@com_github_moby_moby_api//pkg/stdcopy",
        "@com_github_moby_moby_api//types/container",
        "@com_github_moby_moby_api//types/registry",
        "@com_github_moby_moby_client//:client",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "docker_test",
    srcs = ["docker_test.go"],
    exec_properties = {
        # TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/1094): Require less manual configuration here.
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622",
        "test.init-dockerd": "true",
    },
    shard_count = 2,
    tags = ["docker"],
    deps = [
        ":docker",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/status",
        "@com_github_moby_moby_client//:client",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

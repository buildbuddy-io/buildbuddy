load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ociruntime",
    srcs = ["ociruntime.go"],
    embedsrcs = [
        # This is the default seccomp.json file that ships with podman.
        # https://github.com/containers/podman/blob/c510959826cdc55e6a75c40b104a9d1aa28e3632/vendor/github.com/containers/common/pkg/seccomp/seccomp.json
        "seccomp.json",
        "hosts",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "ociruntime_test",
    srcs = ["ociruntime_test.go"],
    data = [":crun"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.EstimatedComputeUnits": "2",
    },
    tags = [
        # TODO: remove manual tag once cgroup_v2_only flag is rolled out
        # to executors. crun doesn't support cgroup v1+v2 "hybrid" mode.
        "manual",
        "no-sandbox",
    ],
    target_compatible_with = ["@platforms//os:linux"],
    x_defs = {
        "crunRlocationpath": "$(rlocationpath :crun)",
    },
    deps = [
        ":ociruntime",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)

alias(
    name = "crun",
    actual = select({
        "@platforms//cpu:x86_64": "@com_github_containers_crun_crun-linux-amd64//file",
        "@platforms//cpu:aarch64": "@com_github_containers_crun_crun-linux-arm64//file",
    }),
)

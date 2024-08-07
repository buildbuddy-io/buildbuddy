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
        "//enterprise/server/remote_execution/cgroup",
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/oci",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/unixcred",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sync//singleflight",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "ociruntime_test",
    srcs = ["ociruntime_test.go"],
    data = [
        ":busybox",
        ":crun",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://gcr.io/flame-public/net-tools@sha256:ac701954d2c522d0d2b5296323127cacaaf77627e69db848a8d6ecb53149d344",
        "test.EstimatedComputeUnits": "2",
    },
    tags = [
        "docker",
        "no-sandbox",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        # TODO: GitHub Actions arm runners are running into cgroup-related issues;
        # fix and re-enable on arm64.
        "@platforms//cpu:x86_64",
    ],
    x_defs = {
        "crunRlocationpath": "$(rlocationpath :crun)",
        "busyboxRlocationpath": "$(rlocationpath :busybox)",
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
        "//server/testutil/testnetworking",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)

alias(
    name = "crun",
    actual = select({
        "@platforms//cpu:x86_64": "@com_github_containers_crun_crun-linux-amd64//file:crun",
        "@platforms//cpu:aarch64": "@com_github_containers_crun_crun-linux-arm64//file:crun",
    }),
)

alias(
    name = "busybox",
    actual = select({
        "@platforms//cpu:x86_64": "@net_busybox_busybox-linux-amd64//file:busybox",
        "@platforms//cpu:aarch64": "@net_busybox_busybox-linux-arm64//file:busybox",
    }),
)

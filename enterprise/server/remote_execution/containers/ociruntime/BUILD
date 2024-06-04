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
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "ociruntime_test",
    srcs = ["ociruntime_test.go"],
    tags = [
        "manual",
        "no-sandbox",
    ],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        ":ociruntime",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

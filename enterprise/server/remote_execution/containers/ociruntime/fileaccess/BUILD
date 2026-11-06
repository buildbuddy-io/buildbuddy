load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "fileaccess",
    srcs = ["fileaccess.go"],
    embedsrcs = ["fileaccess.bpf.o"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/fileaccess",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@com_github_cilium_ebpf//:ebpf",
        "@com_github_cilium_ebpf//link",
        "@com_github_cilium_ebpf//ringbuf",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "fileaccess_test",
    srcs = ["fileaccess_test.go"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        ":fileaccess",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sys//unix",
    ],
)

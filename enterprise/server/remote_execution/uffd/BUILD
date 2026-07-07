load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "uffd",
    srcs = ["uffd.go"],
    cgo = True,
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/snaputil",
        "//server/metrics",
        "//server/util/log",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "uffd_test",
    srcs = ["uffd_test.go"],
    embed = [":uffd"],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/remote_execution/copy_on_write",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sys//unix",
    ],
)

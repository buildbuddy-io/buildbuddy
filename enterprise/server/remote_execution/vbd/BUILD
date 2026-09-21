load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vbd",
    srcs = ["vbd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd",
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_hanwen_go_fuse_v2//fs",
        "@com_github_hanwen_go_fuse_v2//fuse",
    ],
)

go_test(
    name = "vbd_test",
    srcs = ["vbd_test.go"],
    exec_properties = {
        # Test requires mount() privileges.
        "test.workload-isolation-type": "firecracker",
    },
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":vbd",
        "//server/testutil/testfs",
        "//server/testutil/testmount",
        "@com_github_stretchr_testify//require",
    ],
)

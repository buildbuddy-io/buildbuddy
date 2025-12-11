load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "image_verification_test",
    srcs = ["image_verification_test.go"],
    deps = [
        "//enterprise/server/remote_execution/containers/firecracker",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

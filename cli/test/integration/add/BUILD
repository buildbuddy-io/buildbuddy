load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "add_test",
    srcs = ["add_test.go"],
    exec_properties = {
        # `bb add` looks modules up on registry.build and the Bazel Central
        # Registry.
        "test.dockerNetwork": "bridge",
    },
    deps = [
        "//cli/bzlmod",
        "//cli/testutil/testcli",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

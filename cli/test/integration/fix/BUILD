load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//cli:__subpackages__"])

bazel_versions_test(
    name = "fix_test",
    srcs = ["fix_test.go"],
    exec_properties = {
        # Mirror cli/test/integration/cli: bb may reach the network during
        # startup (e.g. for version checks). Tests themselves are network-free.
        "test.dockerNetwork": "bridge",
    },
    rule = go_test,
    deps = [
        "//cli/testutil/testcli",
        "//server/testutil/quarantine",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

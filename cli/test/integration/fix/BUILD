load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "fix_test",
    srcs = ["fix_test.go"],
    exec_properties = {
        # Mirror cli/test/integration/cli: bb may reach the network during
        # startup (e.g. for version checks). The repo-defined //:gazelle tests
        # also fetch pinned BCR deps via real bazel.
        "test.dockerNetwork": "bridge",
    },
    deps = [
        "//cli/testutil/testcli",
        "//server/testutil/quarantine",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

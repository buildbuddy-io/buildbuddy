load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "fix_test",
    srcs = [
        "fix_failures_test.go",
        "fix_test.go",
    ],
    exec_properties = {
        # bb may reach the network during startup (e.g. for version checks),
        # and the Go project tests add rules_go and gazelle with `bb add`,
        # which looks them up on registry.build and the BCR.
        "test.dockerNetwork": "bridge",
    },
    deps = [
        "//cli/bzlmod",
        "//cli/testutil/testcli",
        "//server/testutil/quarantine",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

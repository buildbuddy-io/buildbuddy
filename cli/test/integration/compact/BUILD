load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "compact_test",
    srcs = ["compact_test.go"],
    data = glob(["testdata/**"]),
    deps = [
        "//cli/testutil/testcli",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

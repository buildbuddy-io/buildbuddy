load("@io_bazel_rules_go//go:def.bzl", "go_test")

# One test target per test file.
# gazelle:go_test file

go_test(
    name = "branch_highest_version_test",
    srcs = ["branch_highest_version_test.go"],
    data = ["//tools:branch_highest_version.sh"],
    x_defs = {"branchHighestVersionRlocationpath": "$(rlocationpath //tools:branch_highest_version.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

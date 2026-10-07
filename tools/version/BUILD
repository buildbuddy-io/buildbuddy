load("@io_bazel_rules_go//go:def.bzl", "go_test")

# One test target per script.
# gazelle:go_test file

go_test(
    name = "repo_highest_version_test",
    srcs = ["repo_highest_version_test.go"],
    data = ["repo_highest_version.sh"],
    x_defs = {"repoHighestVersionRlocationpath": "$(rlocationpath :repo_highest_version.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

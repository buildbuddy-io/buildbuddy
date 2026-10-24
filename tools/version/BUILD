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

go_test(
    name = "hosted_label_test",
    srcs = ["hosted_label_test.go"],
    data = ["//tools:hosted_label.sh"],
    x_defs = {"hostedLabelRlocationpath": "$(rlocationpath //tools:hosted_label.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "repo_highest_version_test",
    srcs = ["repo_highest_version_test.go"],
    data = ["//tools:repo_highest_version.sh"],
    x_defs = {"repoHighestVersionRlocationpath": "$(rlocationpath //tools:repo_highest_version.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "version_label_test",
    srcs = ["version_label_test.go"],
    data = [
        "//tools:hosted_label.sh",
        "//tools:version_label.sh",
    ],
    x_defs = {"versionLabelRlocationpath": "$(rlocationpath //tools:version_label.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

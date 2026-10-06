load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

package(default_visibility = ["//visibility:public"])

py_binary(
    name = "check_release_assets_uploaded",
    srcs = ["check_release_assets_uploaded.py"],
    visibility = ["//:__subpackages__"],
    deps = ["@pypi//requests"],
)

py_library(
    name = "tools_py_library",
    srcs = ["check_release_assets_uploaded.py"],
    visibility = ["//:__subpackages__"],
    deps = ["@pypi//requests"],
)

sh_test(
    name = "branch_highest_version_test",
    srcs = ["branch_highest_version_test.sh"],
    data = ["branch_highest_version.sh"],
    env = {"BRANCH_HIGHEST_VERSION": "$(rootpath :branch_highest_version.sh)"},
)

go_test(
    name = "tools_test",
    srcs = [
        "repo_highest_version_test.go",
        "version_script_helpers_test.go",
    ],
    data = ["repo_highest_version.sh"],
    x_defs = {"repoHighestVersionRlocationpath": "$(rlocationpath :repo_highest_version.sh)"},
    deps = [
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

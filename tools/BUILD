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
    env = {"BRANCH_HIGHEST_VERSION": "$(rlocationpath :branch_highest_version.sh)"},
    deps = ["@bazel_tools//tools/bash/runfiles"],
)

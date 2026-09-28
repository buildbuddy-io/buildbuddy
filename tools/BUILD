load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

py_test(
    name = "tag_release_branch_test",
    size = "small",
    srcs = [
        "tag_release_branch.py",
        "tag_release_branch_test.py",
    ],
)

py_test(
    name = "release_tag_test",
    size = "small",
    srcs = ["release_tag_test.py"],
    data = ["//:release.py"],
)

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

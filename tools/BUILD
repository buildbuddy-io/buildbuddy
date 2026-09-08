load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

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

py_binary(
    name = "source_version",
    srcs = ["source_version.py"],
)

py_library(
    name = "source_version_lib",
    srcs = ["source_version.py"],
)

py_test(
    name = "source_version_test",
    size = "small",
    srcs = ["source_version_test.py"],
    deps = [":source_version_lib"],
)

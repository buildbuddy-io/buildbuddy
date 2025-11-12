load("@rules_python//python:defs.bzl", "py_binary", "py_library")

py_binary(
    name = "check_buildx_support",
    srcs = ["check_buildx_support.py"],
    visibility = ["//:__subpackages__"],
    deps = ["@pypi//packaging"],
)

py_library(
    name = "lib_py_library",
    srcs = [
        "__init__.py",
        "check_buildx_support.py",
    ],
    visibility = ["//:__subpackages__"],
    deps = ["@pypi//packaging"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

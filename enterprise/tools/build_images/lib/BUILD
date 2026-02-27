load("@rules_python//python:defs.bzl", "py_binary")

package(default_visibility = ["//enterprise:__subpackages__"])

py_binary(
    name = "check_buildx_support",
    srcs = ["check_buildx_support.py"],
    visibility = ["//:__subpackages__"],
    deps = ["@pypi//packaging"],
)

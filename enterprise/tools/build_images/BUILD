load("@rules_python//python:defs.bzl", "py_library")

# gazelle:resolve py lib.check_buildx_support //enterprise/tools/build_images/lib:check_buildx_support

py_library(
    name = "build_images_py_library",
    srcs = ["build_image.py"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//enterprise/tools/build_images/lib:check_buildx_support",
        "@pypi//requests",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

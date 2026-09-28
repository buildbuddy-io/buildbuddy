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
    name = "version_tag_test",
    srcs = ["version_tag_test.sh"],
    data = [
        "latest_version_tag.sh",
        "version_tag_at_head.sh",
    ],
    env = {
        "LATEST_VERSION_TAG": "$(rootpath :latest_version_tag.sh)",
        "VERSION_TAG_AT_HEAD": "$(rootpath :version_tag_at_head.sh)",
    },
)

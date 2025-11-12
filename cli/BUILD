load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

# gazelle:exclude example_plugins/**/*.py
# gazelle:exclude plugins/**/*.py
# gazelle:default_visibility //cli:__subpackages__
package(default_visibility = ["//cli:__subpackages__"])

alias(
    name = "cli",
    actual = "//cli/cmd/bb",
)

genquery(
    name = "cli_enterprise_deps",
    expression = "filter(//enterprise, deps(//cli))",
    scope = ["//cli"],
    tags = ["manual"],
)

sh_test(
    name = "cli_no_enterprise_deps_test",
    srcs = ["cli_no_enterprise_deps_test.sh"],
    args = ["$(rootpath :cli_enterprise_deps)"],
    data = ["cli_enterprise_deps"],
    tags = [
        # TODO: Drop this tag after updating to Bazel 8.5.0.
        # https://github.com/bazelbuild/bazel/pull/27117
        "public-only",
    ],
)

exports_files(
    srcs = ["install.sh"],
)

py_binary(
    name = "release",
    srcs = ["release.py"],
    visibility = ["//:__subpackages__"],
)

py_library(
    name = "cli_py_library",
    srcs = ["release.py"],
    visibility = ["//:__subpackages__"],
)

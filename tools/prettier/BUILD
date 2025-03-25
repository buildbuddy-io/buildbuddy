load("@npm//:prettier/package_json.bzl", "bin")

package(
    default_visibility = ["//visibility:public"],
)

bin.prettier_binary(
    name = "prettier_bin",
)

sh_binary(
    name = "prettier",
    srcs = ["prettier.sh"],
    data = [
        ":prettier_bin",
    ],
    deps = [
        "@bazel_tools//tools/bash/runfiles",
    ],
)

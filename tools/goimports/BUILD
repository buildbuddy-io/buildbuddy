load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

sh_binary(
    name = "goimports",
    srcs = ["goimports.sh"],
    data = [
        "@org_golang_x_tools//cmd/goimports",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@bazel_tools//tools/bash/runfiles",
    ],
)

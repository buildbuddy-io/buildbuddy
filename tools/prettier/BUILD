sh_binary(
    name = "prettier",
    srcs = ["prettier.sh"],
    data = [
        "@npm//prettier/bin:prettier",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@bazel_tools//tools/bash/runfiles",
    ],
)

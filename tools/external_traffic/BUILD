load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

sh_binary(
    name = "build_abseil",
    srcs = ["build_abseil.sh"],
    data = glob(["abseil_workspace/**"]),
    visibility = ["//visibility:public"],
)

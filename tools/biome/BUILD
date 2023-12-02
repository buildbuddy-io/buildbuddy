sh_binary(
    name = "biome",
    srcs = ["biome.sh"],
    data = [":biome_crossplatform"],
    deps = [
        "@bazel_tools//tools/bash/runfiles",
    ],
)

genrule(
    name = "biome_crossplatform_copy",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["@com_github_biomejs_biome_linux_x86_64//file:downloaded"],
        "//platforms/configs:macos_arm64": ["@com_github_biomejs_biome_darwin_arm64//file:downloaded"],
    }),
    outs = ["biome_crossplatform"],
    cmd_bash = "cp $(SRCS) $@",
)

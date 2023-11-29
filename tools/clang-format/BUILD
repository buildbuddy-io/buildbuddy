genrule(
    name = "clang-format_crossplatform",
    srcs = select({
        # TODO: mac build
        # "@bazel_tools//src/conditions:darwin": ["@org_llvm_llvm_clang-format_darwin-arm64//file:downloaded"],
        "//conditions:default": ["@org_llvm_llvm_clang-format_linux-x86_64//file:downloaded"],
    }),
    outs = ["clang-format"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

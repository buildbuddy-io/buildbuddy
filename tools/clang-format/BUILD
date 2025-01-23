genrule(
    name = "clang-format_crossplatform",
    srcs = select({
        "@platforms//os:macos": ["@org_llvm_llvm_clang-format_macos-x86_64//file:downloaded"],
        "@platforms//os:linux": ["@org_llvm_llvm_clang-format_linux-x86_64//file:downloaded"],
    }),
    outs = ["clang-format"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

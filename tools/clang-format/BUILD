alias(
    name = "clang-format",
    actual = select({
        "@platforms//os:macos": "@org_llvm_llvm_clang-format_macos-x86_64//file:downloaded",
        "@platforms//os:linux": "@org_llvm_llvm_clang-format_linux-x86_64//file:downloaded",
    }),
    visibility = ["//visibility:public"],
)

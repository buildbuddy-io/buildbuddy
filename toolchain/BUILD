package(default_visibility = ["//visibility:public"])

load(":cc_toolchain_config.bzl", "cc_toolchain_config")

cc_toolchain_suite(
    name = "cc",
    toolchains = {
        "linux-cross": ":gcc_toolchain",
        "darwin": ":gcc_toolchain",
        "gcc": ":gcc_toolchain",
        "clang": ":clang_toolchain",
    },
)

filegroup(name = "empty")

cc_toolchain(
    name = "gcc_toolchain",
    all_files = ":empty",
    compiler_files = ":empty",
    dwp_files = ":empty",
    linker_files = ":empty",
    objcopy_files = ":empty",
    strip_files = ":empty",
    supports_param_files = 0,
    toolchain_config = ":gcc_toolchain_config",
    toolchain_identifier = "gcc-toolchain",
)

cc_toolchain_config(
    name = "gcc_toolchain_config",
    compiler_path = "/usr/bin/g++",
    warnings = [
        "all",
        "error",
        "extra",
        "no-sign-compare",
        "no-unused-but-set-parameter",
        "no-unused-parameter",
        "no-unused-variable",
        "uninitialized",
    ],
)

cc_toolchain(
    name = "clang_toolchain",
    all_files = ":empty",
    compiler_files = ":empty",
    dwp_files = ":empty",
    linker_files = ":empty",
    objcopy_files = ":empty",
    strip_files = ":empty",
    supports_param_files = 0,
    toolchain_config = ":clang_toolchain_config",
    toolchain_identifier = "clang-toolchain",
)

cc_toolchain_config(
    name = "clang_toolchain_config",
    compiler_path = "/usr/bin/clang++",
    warnings = [
        "all",
        "error",
        "extra",
        "no-sign-compare",
        "no-unused-parameter",
        "no-unused-variable",
        "no-overloaded-shift-op-parentheses",  # because of ASSERT macro.
        "no-overloaded-virtual",  # TODO fix this. The visitor pattern is gross.
        "uninitialized",
    ],
)

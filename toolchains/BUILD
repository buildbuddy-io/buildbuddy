load("@rules_cc//cc:defs.bzl", "cc_toolchain")

# TODO(bduffany): The sh_toolchain config here is a workaround for
# https://github.com/aspect-build/rules_swc/issues/20
# We should probably either move these to the buildbuddy-toolchain repo
# or add a symlink from /usr/bin/bash -> /bin/bash to remove the need for these.
load("@bazel_tools//tools/sh:sh_toolchain.bzl", "sh_toolchain")
load(":cc_toolchain_config.bzl", "cc_toolchain_config")

sh_toolchain(
    name = "bash_rbe",
    path = "/bin/bash",
)

toolchain(
    name = "sh_toolchain",
    toolchain = ":bash_rbe",
    toolchain_type = "@bazel_tools//tools/sh:toolchain_type",
)

# A duplicate of @buildbuddy_toolchain//:ubuntu_cc_toolchain
# with correct cc_compiler constraint value.
# TODO(sluongng): upstream this to buildbuddy-toolchain
toolchain(
    name = "ubuntu_cc_toolchain",
    exec_compatible_with = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
        # This is not needed for the toolchain itself,
        # but it helps us differentiate between the local platform and the remote RBE platform.
        #
        # For local builds, we do NOT want to use this toolchain as the gcc and glibc versions might differ.
        "@bazel_tools//tools/cpp:gcc",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    toolchain = "@buildbuddy_toolchain//:ubuntu_local_cc_toolchain",
    toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
)

toolchain(
    name = "cosmo_toolchain",
    toolchain = ":cosmo_cc_toolchain",
    toolchain_type = "@bazel_tools//tools/cpp:toolchain_type",
)

# cc_toolchain(
    # name = "local",
    # 
# )

cc_toolchain(
    name = "cosmo_cc_toolchain",
    # all_files = ":compiler_deps",
    # ar_files = ":compiler_deps",
    # as_files = ":compiler_deps",
    # compiler_files = ":compiler_deps",
    # dwp_files = ":empty",
    # linker_files = ":compiler_deps",
    # objcopy_files = ":empty",
    # strip_files = ":empty",
    all_files = "@cosmocc//:x86_64-full",
    ar_files = "@cosmocc//:x86_64-full",
    as_files = "@cosmocc//:x86_64-full",
    compiler_files = "@cosmocc//:x86_64-full",
    dwp_files = ":empty",
    linker_files = "@cosmocc//:x86_64-full",
    objcopy_files = "@cosmocc//:x86_64-full",
    static_runtime_lib = "@cosmocc//:x86_64-libstdc++.a",
    strip_files = "@cosmocc//:x86_64-full",
    # supports_param_files = 1,
    supports_param_files = 0,
    toolchain_config = ":cosmo_cc_toolchain_config",
)

cc_toolchain_config(
    name = "cosmo_cc_toolchain_config",
    abi_libc_version = "unknown",
    abi_version = "unknown",
    builtin_sysroot = "",
    compile_flags = [
        # "--strace",
        "-isystem",
        "external/cosmocc/include",
        "-include",
        "external/cosmocc/include/libc/integral/normalize.inc",
    ] + [
        "-U_FORTIFY_SOURCE",
        "-fstack-protector",
        "-Wall",
        "-Wunused-but-set-parameter",
        "-Wno-free-nonheap-object",
        "-pass-exit-codes",
    ] + [
        "-fno-pie",
        "-nostdinc",
        "-fno-math-errno",
        "-fportcosmo",
        "-fno-dwarf2-cfi-asm",
        "-fno-unwind-tables",
        "-fno-asynchronous-unwind-tables",
        "-fno-semantic-interposition",
        "-fno-optimize-sibling-calls",
        "-mno-omit-leaf-frame-pointer",
        "-fno-exceptions",
        "-mno-red-zone",
        "-D__COSMOPOLITAN__",
        "-D__COSMOCC__",
        "-D__FATCOSMOCC__",
        "-mno-tls-direct-seg-refs",
        # "-ffixed-x18",
        # "-ffixed-x28",
        # "-mno-outline-atomics",
    ] + [
        "-fno-omit-frame-pointer",
    ],
    compiler = "cosmo_cc-11.2.0",
    coverage_compile_flags = ["--coverage"],
    coverage_link_flags = ["--coverage"],
    cpu = "k8",
    cxx_flags = [
        # "-std=c++17",
        "-fno-rtti",
        "-fuse-cxa-atexit",
    ],
    dbg_compile_flags = ["-g"],
    host_system_name = "local",
    link_flags = [
        "-Lexternal/cosmocc/x86_64-linux-cosmo/lib",
        # "-static",
        # "-nostdlib",
        # "--no-demangle",
        # "-no-pie",
        # "--gc-sections",
        "-Texternal/cosmocc/x86_64-linux-cosmo/lib/ape.lds",
        "-zcommon-page-size=4096",
        "-zmax-page-size=16384",
        "-no-as-needed",
        "-znorelro",
        "-znow",
        "-t",
    ],
    link_libs = [
        "-lc",
        "-lcosmo",
        "-lcxx",
        "-ldl",
        "-lgcc_s",
        "-lm",
        "-lresolv",
        "-lrt",
        "-lstdc++",
        "external/cosmocc/x86_64-linux-cosmo/lib/ape-no-modify-self.o",
        "external/cosmocc/x86_64-linux-cosmo/lib/ape.o",
        "external/cosmocc/x86_64-linux-cosmo/lib/crt.o",
    ],
    opt_compile_flags = [
        "-O2",
        "-D_FORTIFY_SOURCE=1",
        "-DNDEBUG",
        "-ffunction-sections",
        "-fdata-sections",
    ],
    opt_link_flags = ["--gc-sections"],
    target_libc = "cosmo_cc",
    target_system_name = "local",
    toolchain_identifier = "cosmo_cc",
    unfiltered_compile_flags = [
        "-fno-canonical-system-headers",
        "-Wno-builtin-macro-redefined",
        "-D__DATE__=\"redacted\"",
        "-D__TIMESTAMP__=\"redacted\"",
        "-D__TIME__=\"redacted\"",
    ],
)

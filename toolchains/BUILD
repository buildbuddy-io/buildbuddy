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
    supports_param_files = 1,
    toolchain_config = ":cosmo_cc_toolchain_config",
)

cc_toolchain_config(
    name = "cosmo_cc_toolchain_config",
    abi_libc_version = "unknown",
    abi_version = "unknown",
    builtin_sysroot = "",
    compile_flags = [
        "--strace",
        "-isystem",
        "external/cosmocc/include",
        "-U_FORTIFY_SOURCE",
        "-fstack-protector",
        "-Wall",
        "-Wunused-but-set-parameter",
        "-Wno-free-nonheap-object",
        "-fno-omit-frame-pointer",
    ],
    compiler = "cosmo_cc-11.2.0",
    coverage_compile_flags = ["--coverage"],
    coverage_link_flags = ["--coverage"],
    cpu = "k8",
    cxx_flags = ["-std=c++17"],
    dbg_compile_flags = ["-g"],
    host_system_name = "local",
    link_flags = [
        "-fuse-ld=gold",
        "-Wl,-no-as-needed",
        "-Wl,-z,relro,-z,now",
        "-pass-exit-codes",
        "-L external/cosmocc/x86_64-linux-cosmo/lib",
        "-lstdc++",
        "-lm",
    ],
    link_libs = [],
    opt_compile_flags = [
        "-g0",
        "-O2",
        "-D_FORTIFY_SOURCE=1",
        "-DNDEBUG",
        "-ffunction-sections",
        "-fdata-sections",
    ],
    opt_link_flags = ["-Wl,--gc-sections"],
    supports_start_end_lib = True,
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

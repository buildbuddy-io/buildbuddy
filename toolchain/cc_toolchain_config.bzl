load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
    "with_feature_set",
)
load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")

def std_lib_version(version):
    return feature(
        name = "std_lib_version",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.cpp_compile,
                    ACTION_NAMES.cpp_header_parsing,
                    ACTION_NAMES.cpp_module_compile,
                    ACTION_NAMES.cpp_module_codegen,
                ],
                flag_groups = [flag_group(flags = ["-std=c++" + version])],
            ),
        ],
    )

def mode_dependent_flags(dictionary):
    return feature(
        name = "mode_dependent_flags",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [ACTION_NAMES.c_compile, ACTION_NAMES.cpp_compile],
                flag_groups = [flag_group(flags = value)],
                with_features = [with_feature_set(features = [key])],
            )
            for (key, value) in dictionary.items()
        ],
    )

def compiler_flags(name, prefix_char, flags):
    return feature(
        name = name,
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.cpp_compile,
                    ACTION_NAMES.cpp_header_parsing,
                    ACTION_NAMES.cpp_module_compile,
                    ACTION_NAMES.cpp_module_codegen,
                ],
                flag_groups = [flag_group(flags = ["-" + prefix_char + f for f in flags])],
            ),
        ],
    )

def compiler_features(fs):
    return compiler_flags("compiler_features", "f", fs)

def compiler_warnings(ws):
    return compiler_flags("compiler_warnings", "W", ws)

def linkopts(ls):
    return feature(
        name = "linkopts",
        enabled = True,
        flag_sets = [
            flag_set(
                actions = [
                    ACTION_NAMES.cpp_link_dynamic_library,
                    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
                    ACTION_NAMES.cpp_link_executable,
                ],
                flag_groups = [flag_group(flags = ls)],
            ),
        ],
    )

def _impl(ctx):
    tool_paths = [
        tool_path(name = "gcc", path = ctx.attr.compiler_path),
        tool_path(name = "ld", path = "/usr/bin/ld"),
        tool_path(name = "ar", path = "/usr/bin/ar"),
        tool_path(name = "cpp", path = "/bin/false"),
        tool_path(name = "gcov", path = "/bin/false"),
        tool_path(name = "nm", path = "/bin/false"),
        tool_path(name = "objdump", path = "/bin/false"),
        tool_path(name = "strip", path = "/bin/false"),
    ]

    features = [
        std_lib_version("17"),
        compiler_warnings(ctx.attr.warnings),
        compiler_features([
            "diagnostics-color=always",
            "no-exceptions",
        ]),
        linkopts([
            "-ldl",
            "-pthread",
            "-lffi",
            "-rdynamic",
        ]),
        mode_dependent_flags({
            "dbg": ["-g", "-O0", "-DICARUS_DEBUG"],
            "opt": ["-O2", "-DNDEBUG"],
        }),
    ]

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "asmjs-toolchain",
        host_system_name = "i686-unknown-linux-gnu",
        target_system_name = "asmjs-unknown-emscripten",
        target_cpu = "gcc",
        target_libc = "unknown",
        compiler = "gcc",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        cxx_builtin_include_directories = ["/usr/lib/llvm-9", "/usr/include", "/usr/lib/gcc"],
        tool_paths = tool_paths,
        features = features + [
            feature(name = "dbg"),
            feature(name = "fastbuild"),
            feature(name = "opt"),
        ],
    )

cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {
        "compiler_path": attr.string(),
        "warnings": attr.string_list(),
    },
    provides = [CcToolchainConfigInfo],
)

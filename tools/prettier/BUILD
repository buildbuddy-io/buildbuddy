load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")
load("@npm//@bazel/typescript:index.bzl", "ts_library")

ts_library(
    name = "common",
    srcs = ["common.ts"],
    deps = [
        "@npm//@types/node",
        "@npm//@types/prettier",
        "@npm//prettier",
        "@npm//tslib",
    ],
)

ts_library(
    name = "check_tool",
    srcs = ["check.ts"],
    deps = [
        ":common",
        "@npm//@types/node",
        "@npm//@types/prettier",
        "@npm//chalk",
        "@npm//prettier",
    ],
)

ts_library(
    name = "fix_tool",
    srcs = ["fix.ts"],
    deps = [
        ":common",
        "@npm//@types/node",
        "@npm//@types/prettier",
        "@npm//chalk",
        "@npm//prettier",
    ],
)

nodejs_binary(
    name = "fix",
    data = [":fix_tool"],
    entry_point = ":fix.ts",
)

nodejs_binary(
    name = "check",
    data = [":check_tool"],
    entry_point = ":check.ts",
)

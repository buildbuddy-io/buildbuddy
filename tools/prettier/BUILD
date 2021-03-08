load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")
load("@npm//@bazel/typescript:index.bzl", "ts_library")

ts_library(
    name = "common",
    srcs = ["common.js"],
    deps = [
        "@npm//@types/node",
        "@npm//prettier",
    ],
)

ts_library(
    name = "check_lib",
    srcs = ["check.ts"],
    deps = [
        ":common",
        "@npm//@types/node",
        "@npm//chalk",
        "@npm//prettier",
    ],
)

ts_library(
    name = "fix_lib",
    srcs = ["fix.ts"],
    deps = [
        ":common",
        "@npm//@types/node",
        "@npm//chalk",
        "@npm//prettier",
    ],
)

nodejs_binary(
    name = "fix",
    data = [":fix_lib"],
    entry_point = "fix.js",
)

nodejs_binary(
    name = "check",
    data = [":check_lib"],
    entry_point = "check.js",
)

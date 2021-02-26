load("@build_bazel_rules_nodejs//:index.bzl", "js_library", "nodejs_binary")

js_library(
    name = "common",
    srcs = ["common.js"],
    visibility = ["//visibility:public"],
)

nodejs_binary(
    name = "fix",
    data = [
        ":common",
        "@npm//prettier",
    ],
    entry_point = "fix.js",
    visibility = ["//visibility:public"],
)

nodejs_binary(
    name = "check",
    data = [
        ":common",
        "@npm//prettier",
    ],
    entry_point = "check.js",
    visibility = ["//visibility:public"],
)

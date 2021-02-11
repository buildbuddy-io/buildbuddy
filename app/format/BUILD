load("@npm//@bazel/typescript:index.bzl", "ts_library")
load("@npm//@bazel/jasmine:index.bzl", "jasmine_node_test")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = ["format.tsx"],
    deps = [
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//long",
        "@npm//moment",
    ],
)

ts_library(
    name = "format_test_ts",
    srcs = ["format_test.ts"],
    deps = [
        ":format",
        "@npm//@types/jasmine",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

jasmine_node_test(
    name = "format_test",
    deps = [":format_test_ts"],
)

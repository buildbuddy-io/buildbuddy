load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")
load("@npm//@bazel/jasmine:index.bzl", "jasmine_node_test")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = ["format.tsx"],
    deps = [
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//date-fns",
        "@npm//long",
        "@npm//moment",
        "@npm//tslib",
    ],
)

ts_jasmine_node_test(
    name = "format_test",
    srcs = ["format_test.ts"],
    deps = [
        ":format",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

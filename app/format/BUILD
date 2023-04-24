load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = ["format.tsx"],
    strict = True,
    deps = [
        "@npm//@types/long",
        "@npm//@types/moment",
        "@npm//date-fns",
        "@npm//long",
        "@npm//moment",
    ],
)

ts_jasmine_node_test(
    name = "format_test",
    srcs = ["format_test.ts"],
    strict = True,
    deps = [
        ":format",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

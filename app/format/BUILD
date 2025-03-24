load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = ["format.tsx"],
    deps = [
        "//app/util:proto",
        "//proto:duration_ts_proto",
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
    deps = [
        ":format",
        "@npm//@types/long",
        "@npm//long",
        "@npm//tslib",
    ],
)

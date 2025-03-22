load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "format",
    srcs = ["format.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/@types/moment",
        "//:node_modules/date-fns",
        "//:node_modules/long",
        "//:node_modules/moment",
        "//app/util:proto",
        "//proto:duration_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "format_test",
    srcs = ["format_test.ts"],
    deps = [
        ":format",
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//:node_modules/tslib",
    ],
)

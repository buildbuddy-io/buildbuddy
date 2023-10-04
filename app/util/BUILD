load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "animated_value",
    srcs = ["animated_value.ts"],
    deps = ["//app/util:math"],
)

ts_library(
    name = "animation_loop",
    srcs = ["animation_loop.ts"],
    deps = ["//app/util:time_delta"],
)

ts_library(
    name = "async",
    srcs = ["async.ts"],
)

ts_jasmine_node_test(
    name = "async_test",
    srcs = ["async_test.ts"],
    deps = [
        ":async",
        "@npm//tslib",
    ],
)

ts_library(
    name = "clipboard",
    srcs = ["clipboard.ts"],
)

ts_library(
    name = "color",
    srcs = ["color.ts"],
)

ts_library(
    name = "dom",
    srcs = ["dom.ts"],
)

ts_library(
    name = "errors",
    srcs = ["errors.ts"],
)

ts_library(
    name = "git",
    srcs = ["git.ts"],
    deps = ["@npm//tslib"],
)

ts_jasmine_node_test(
    name = "git_test",
    srcs = ["git_test.ts"],
    deps = [
        ":git",
    ],
)

ts_library(
    name = "scroller",
    srcs = ["scroller.ts"],
    deps = [
        "//app/util:animated_value",
        "//app/util:animation_loop",
    ],
)

ts_library(
    name = "time_delta",
    srcs = ["time_delta.ts"],
)

ts_library(
    name = "math",
    srcs = ["math.ts"],
)

ts_library(
    name = "memo",
    srcs = ["memo.ts"],
)

ts_library(
    name = "proto",
    srcs = ["proto.ts"],
    deps = [
        "//proto:duration_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/long",
        "@npm//long",
    ],
)

ts_library(
    name = "lcov",
    srcs = ["lcov.ts"],
)

ts_library(
    name = "platform",
    srcs = ["platform.ts"],
)

ts_library(
    name = "canvas",
    srcs = ["canvas.ts"],
)

load("//rules/typescript:index.bzl", "ts_jasmine_node_test", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "animated_value",
    srcs = ["animated_value.ts"],
    strict = True,
    deps = ["//app/util:math"],
)

ts_library(
    name = "animation_loop",
    srcs = ["animation_loop.ts"],
    strict = True,
    deps = ["//app/util:time_delta"],
)

ts_library(
    name = "async",
    srcs = ["async.ts"],
    strict = True,
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
    strict = True,
)

ts_library(
    name = "color",
    srcs = ["color.ts"],
    strict = True,
)

ts_library(
    name = "dom",
    srcs = ["dom.ts"],
    strict = True,
)

ts_library(
    name = "errors",
    srcs = ["errors.ts"],
    strict = True,
)

ts_library(
    name = "git",
    srcs = ["git.ts"],
    strict = True,
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
    strict = True,
    deps = [
        "//app/util:animated_value",
        "//app/util:animation_loop",
    ],
)

ts_library(
    name = "time_delta",
    srcs = ["time_delta.ts"],
    strict = True,
)

ts_library(
    name = "math",
    srcs = ["math.ts"],
    strict = True,
)

ts_library(
    name = "memo",
    srcs = ["memo.ts"],
    strict = True,
)

ts_library(
    name = "proto",
    srcs = ["proto.ts"],
    strict = True,
    deps = [
        "//proto:duration_ts_proto",
        "//proto:timestamp_ts_proto",
        "@npm//@types/long",
        "@npm//long",
    ],
)

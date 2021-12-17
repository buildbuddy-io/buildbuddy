load("@npm//@bazel/typescript:index.bzl", "ts_library")
load("@npm//@bazel/jasmine:index.bzl", "jasmine_node_test")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "animated_value",
    srcs = ["animated_value.ts"],
    deps = [
        ":math",
        "@npm//tslib",
    ],
)

ts_library(
    name = "animation_loop",
    srcs = ["animation_loop.ts"],
    deps = [
        ":time_delta",
        "@npm//tslib",
    ],
)

ts_library(
    name = "async",
    srcs = ["async.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "async_test_ts",
    srcs = ["async_test.ts"],
    deps = [
        ":async",
        "@npm//@types/jasmine",
        "@npm//tslib",
    ],
)

jasmine_node_test(
    name = "async_test",
    templated_args = ["--bazel_patch_module_resolver"],
    deps = [":async_test_ts"],
)

ts_library(
    name = "cache",
    srcs = ["cache.ts"],
    deps = [
        ":download",
        "//app/capabilities",
        "//app/service",
        "//proto:remote_execution_ts_proto",
    ],
)

ts_library(
    name = "clipboard",
    srcs = ["clipboard.ts"],
)

ts_library(
    name = "dom",
    srcs = ["dom.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "download",
    srcs = ["download.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "errors",
    srcs = ["errors.ts"],
)

ts_library(
    name = "time_delta",
    srcs = ["time_delta.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "math",
    srcs = ["math.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "memo",
    srcs = ["memo.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "proto",
    srcs = ["proto.ts"],
    deps = [
        "//proto:timestamp_ts_proto",
    ],
)

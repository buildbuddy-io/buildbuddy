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
        "//:node_modules/tslib",
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
    deps = [
        "//proto:grpc_code_ts_proto",
        "//proto:grpc_status_ts_proto",
    ],
)

ts_library(
    name = "git",
    srcs = ["git.ts"],
    deps = ["//:node_modules/tslib"],
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
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//:node_modules/protobufjs",
        "//proto:duration_ts_proto",
        "//proto:timestamp_ts_proto",
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

ts_library(
    name = "popup",
    srcs = ["popup.ts"],
)

ts_library(
    name = "exit_codes",
    srcs = ["exit_codes.ts"],
)

ts_library(
    name = "react",
    srcs = ["react.tsx"],
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/react",
    ],
)

ts_library(
    name = "cache",
    srcs = ["cache.tsx"],
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/long",
        "//:node_modules/tslib",
        "//proto:build_event_stream_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:resource_ts_proto",
    ],
)

ts_jasmine_node_test(
    name = "cache_test",
    srcs = ["cache_test.ts"],
    deps = [
        ":cache",
        "//:node_modules/tslib",
    ],
)

ts_library(
    name = "rpc",
    srcs = ["rpc.ts"],
    deps = [
        "//app/service:rpc_service",
        "//app/util:errors",
        "//proto:grpc_code_ts_proto",
        "//proto:grpc_error_details_ts_proto",
        "//proto:grpc_status_ts_proto",
    ],
)

ts_library(
    name = "remote_runner",
    srcs = ["remote_runner.tsx"],
    deps = [
        "//:node_modules/tslib",
        "//app/errors:error_service",
        "//app/invocation:invocation_model",
        "//app/service:rpc_service",
        "//proto:git_ts_proto",
        "//proto:github_ts_proto",
        "//proto:remote_execution_ts_proto",
        "//proto:runner_ts_proto",
    ],
)

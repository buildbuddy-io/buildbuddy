load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "animation",
    srcs = ["animation.ts"],
    deps = [
        ":math",
        ":time",
        "@npm//tslib",
    ],
)

ts_library(
    name = "dispose"
    files = ["dispose.ts"]
    deps = [
        ":events",
        "@npm//tslib",
    ]
)

ts_library(
    name = "dom"
    files = ["dom.ts"]
    deps = [
        "@npm//tslib",
    ]
)

ts_library(
    name = "events"
    files = ["events.ts"]
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ]
)

ts_library(
    name = "math",
    srcs = ["math.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "mouse",
    srcs = ["mouse.ts"],
    deps = [
        "@npm//tslib",
    ],
)

ts_library(
    name = "time",
    srcs = ["time.ts"],
    deps = [
        "@npm//tslib",
    ],
)

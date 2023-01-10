load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "tooltip",
    srcs = ["tooltip.tsx"],
    strict = True,
    deps = [
        "//app/util:math",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//react",
        "@npm//react-dom",
        "@npm//tslib",
    ],
)

load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "tooltip",
    srcs = ["tooltip.tsx"],
    strict = True,
    deps = [
        "//app/util:math",
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-dom",
        "//:node_modules/react",
        "//:node_modules/react-dom",
        "//:node_modules/tslib",
    ],
)

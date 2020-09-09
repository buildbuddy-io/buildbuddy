load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    deps = [
        "//app/components/scrollbar",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:dom",
        "//app/util:math",
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
    ],
)

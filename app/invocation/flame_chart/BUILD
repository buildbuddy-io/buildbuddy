load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "flame_chart",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    deps = [
        "//app/components/flame_chart",
        "//app/util:memo",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

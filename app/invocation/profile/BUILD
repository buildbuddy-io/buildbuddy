load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "profile",
    srcs = glob(["*.ts", "*.tsx"]),
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
        "//app/components/scrollbar",
        "//app/util:animated_value",
        "//app/util:animation_loop",
        "//app/util:dom",
        "//app/util:math",
    ],
)

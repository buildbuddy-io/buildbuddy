load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "errors",
    srcs = glob([
        "*.tsx",
        "*.ts",
    ]),
    deps = [
        "//app/util:errors",
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
    ],
)

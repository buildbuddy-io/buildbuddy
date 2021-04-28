load("@npm//@bazel/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "alert",
    srcs = glob([
        "*.tsx",
        "*.ts",
    ]),
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//rxjs",
    ],
)

exports_files(srcs = glob(["*.css"]))

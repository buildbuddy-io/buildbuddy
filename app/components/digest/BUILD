load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "digest",
    srcs = ["digest.tsx"],
    deps = [
        "//app/format",
        "@npm//@types/long",
        "@npm//@types/react",
        "@npm//long",
        "@npm//react",
        "@npm//tslib",
    ],
)

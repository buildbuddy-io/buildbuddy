load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "popup",
    srcs = ["popup.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "slider",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//@types/react-slider",
        "@npm//react",
        "@npm//react-slider",
        "@npm//tslib",
    ],
)

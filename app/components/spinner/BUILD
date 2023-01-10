load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "spinner",
    srcs = glob(["*.tsx"]),
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

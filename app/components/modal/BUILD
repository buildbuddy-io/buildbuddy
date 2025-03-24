load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "modal",
    srcs = ["modal.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//@types/react-modal",
        "@npm//react",
        "@npm//react-modal",
        "@npm//tslib",
    ],
)

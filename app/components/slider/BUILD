load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "slider",
    srcs = ["slider.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-slider",
        "//:node_modules/react",
        "//:node_modules/react-slider",
        "//:node_modules/tslib",
    ],
)

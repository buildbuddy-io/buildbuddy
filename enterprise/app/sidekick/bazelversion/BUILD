load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(glob(["*.css"]))

ts_library(
    name = "bazelversion",
    srcs = ["bazelversion.tsx"],
    deps = [
        "@npm//@types/react",
        "@npm//monaco-editor",
        "@npm//react",
        "@npm//tslib",
    ],
)

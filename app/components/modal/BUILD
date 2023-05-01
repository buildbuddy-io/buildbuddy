load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "modal",
    srcs = ["modal.tsx"],
    strict = True,
    deps = [
        "//:node_modules/@types/react",
        "//:node_modules/@types/react-modal",
        "//:node_modules/react",
        "//:node_modules/react-modal",
        "//:node_modules/tslib",
    ],
)

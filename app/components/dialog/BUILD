load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "dialog",
    srcs = ["dialog.tsx"],
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

ts_library(
    name = "simple_modal_dialog",
    srcs = ["simple_modal_dialog.tsx"],
    strict = True,
    deps = [
        ":dialog",
        "//app/components/button",
        "//app/components/modal",
        "//app/components/spinner",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

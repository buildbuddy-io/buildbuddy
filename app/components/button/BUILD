load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "button",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    strict = True,
    deps = [
        "//app/components/checkbox",
        "//app/components/link",
        "@npm//@types/react",
        "@npm//react",
        "@npm//tslib",
    ],
)

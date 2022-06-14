load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "button",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    deps = [
        "//app/components/checkbox",
        "//app/components/link",
        "//:node_modules/@types/react",
        "//:node_modules/react",
        "//:node_modules/tslib",
    ],
)

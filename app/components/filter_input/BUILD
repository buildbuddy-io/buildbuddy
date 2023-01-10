load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files(glob(["*.css"]))

ts_library(
    name = "filter_input",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    strict = True,
    deps = [
        "@npm//@types/react",
        "@npm//lucide-react",
        "@npm//react",
        "@npm//tslib",
    ],
)

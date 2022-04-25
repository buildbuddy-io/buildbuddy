load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "scrollbar",
    srcs = glob([
        "*.ts",
        "*.tsx",
    ]),
    deps = [
        "@npm//@types/react",
        "@npm//@types/uuid",
        "@npm//react",
        "@npm//rxjs",
        "@npm//tslib",
        "@npm//uuid",
    ],
)

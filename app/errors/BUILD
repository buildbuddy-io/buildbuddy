load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "errors",
    srcs = glob([
        "*.tsx",
        "*.ts",
    ]),
    strict = True,
    deps = [
        "//app/alert",
        "//app/router",
        "//app/util:errors",
    ],
)

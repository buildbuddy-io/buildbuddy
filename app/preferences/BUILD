load("//rules/typescript:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "preferences",
    srcs = glob(["*.ts"]),
    strict = True,
    deps = [
        "//app/capabilities",
    ],
)
